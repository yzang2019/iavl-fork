package iavl

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	db "github.com/tendermint/tm-db"
)

// maxBatchSize is the maximum size of the import batch before flushing it to the database
const maxBatchSize = 10000

// ErrNoImport is returned when calling methods on a closed importer
var ErrNoImport = errors.New("no import in progress")

// Importer imports data into an empty MutableTree. It is created by MutableTree.Import(). Users
// must call Close() when done.
//
// ExportNodes must be imported in the order returned by Exporter, i.e. depth-first post-order (LRN).
//
// Importer is not concurrency-safe, it is the caller's responsibility to ensure the tree is not
// modified while performing an import.
type Importer struct {
	tree       *MutableTree
	version    int64
	batch      db.Batch
	batchMutex sync.RWMutex
	batchSize  uint32
	stack      []*Node
	chBatch    chan db.Batch
	chNode     chan Node
	chDataNode chan Node
}

// newImporter creates a new Importer for an empty MutableTree.
//
// version should correspond to the version that was initially exported. It must be greater than
// or equal to the highest ExportNode version number given.
func newImporter(tree *MutableTree, version int64) (*Importer, error) {
	if version < 0 {
		return nil, errors.New("imported version cannot be negative")
	}
	if tree.ndb.latestVersion > 0 {
		return nil, errors.Errorf("found database at version %d, must be 0", tree.ndb.latestVersion)
	}
	if !tree.IsEmpty() {
		return nil, errors.New("tree must be empty")
	}

	var importer = &Importer{
		tree:       tree,
		version:    version,
		batch:      tree.ndb.db.NewBatch(),
		batchMutex: sync.RWMutex{},
		stack:      make([]*Node, 0, 8),
		chBatch:    make(chan db.Batch, 1),
		chNode:     make(chan Node, maxBatchSize),
		chDataNode: make(chan Node, maxBatchSize),
	}
	for i := 0; i < 8; i++ {
		go periodicBatchCommit(importer)
	}

	for i := 0; i < 4; i++ {
		go serializeAsync(importer)
	}
	for i := 0; i < 4; i++ {
		go writeNodeData(importer)
	}

	return importer, nil
}

func periodicBatchCommit(i *Importer) {
	for i.batch != nil {
		select {
		case nextBatch := <-i.chBatch:
			batchWriteStart := time.Now().UnixMicro()
			err := nextBatch.Write()
			if err != nil {
				panic(err)
			}
			fmt.Println("Closing batch after batch write done")
			nextBatch.Close()
			batchWriteEnd := time.Now().UnixMicro()
			batchCommitLatency := batchWriteEnd - batchWriteStart
			fmt.Printf("[IAVL IMPORTER] Batch commit latency: %d\n", batchCommitLatency/1000)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	fmt.Printf("[IAVL IMPORTER] Shutting down the batch commit thread\n")
}

func serializeAsync(i *Importer) {
	for i.batch != nil {
		select {
		case node := <-i.chNode:
			err := node.validate()
			if err != nil {
				panic(err)
			}

			var buf bytes.Buffer
			err = node.writeBytes(&buf)
			node.data = buf.Bytes()
			if err != nil {
				panic(err)
			}
			i.chDataNode <- node
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func writeNodeData(i *Importer) {
	for i.batch != nil {
		select {
		case node := <-i.chDataNode:
			i.batchMutex.RLock()
			if i.batch != nil {
				err := i.batch.Set(i.tree.ndb.nodeKey(node.hash), node.data)
				if err != nil {
					panic(err)
				}
			}
			i.batchMutex.RUnlock()
			i.batchMutex.Lock()
			i.batchSize++
			if i.batchSize >= maxBatchSize {
				i.chBatch <- i.batch
				i.batch = i.tree.ndb.db.NewBatch()
				i.batchSize = 0
			}
			i.batchMutex.Unlock()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Close frees all resources. It is safe to call multiple times. Uncommitted nodes may already have
// been flushed to the database, but will not be visible.
func (i *Importer) Close() {
	i.batchMutex.Lock()
	defer i.batchMutex.Unlock()
	fmt.Println("Acquired lock in close")

	if i.batch != nil {
		fmt.Println("Closing batch now!!!")
		i.batch.Close()
	}
	i.batch = nil
	i.tree = nil

}

// Add adds an ExportNode to the import. ExportNodes must be added in the order returned by
// Exporter, i.e. depth-first post-order (LRN). Nodes are periodically flushed to the database,
// but the imported version is not visible until Commit() is called.
func (i *Importer) Add(exportNode *ExportNode) error {

	if i.tree == nil {
		return ErrNoImport
	}
	if exportNode == nil {
		return errors.New("node cannot be nil")
	}
	if exportNode.Version > i.version {
		return errors.Errorf("node version %v can't be greater than import version %v",
			exportNode.Version, i.version)
	}

	node := &Node{
		key:     exportNode.Key,
		value:   exportNode.Value,
		version: exportNode.Version,
		height:  exportNode.Height,
	}

	// We build the tree from the bottom-left up. The stack is used to store unresolved left
	// children while constructing right children. When all children are built, the parent can
	// be constructed and the resolved children can be discarded from the stack. Using a stack
	// ensures that we can handle additional unresolved left children while building a right branch.
	//
	// We don't modify the stack until we've verified the built node, to avoid leaving the
	// importer in an inconsistent state when we return an error.
	stackSize := len(i.stack)
	switch {
	case stackSize >= 2 && i.stack[stackSize-1].height < node.height && i.stack[stackSize-2].height < node.height:
		node.leftNode = i.stack[stackSize-2]
		node.leftHash = node.leftNode.hash
		node.rightNode = i.stack[stackSize-1]
		node.rightHash = node.rightNode.hash
	case stackSize >= 1 && i.stack[stackSize-1].height < node.height:
		node.leftNode = i.stack[stackSize-1]
		node.leftHash = node.leftNode.hash
	}

	if node.height == 0 {
		node.size = 1
	}
	if node.leftNode != nil {
		node.size += node.leftNode.size
	}
	if node.rightNode != nil {
		node.size += node.rightNode.size
	}
	node._hash()

	i.chNode <- *node

	// Update the stack now that we know there were no errors
	switch {
	case node.leftHash != nil && node.rightHash != nil:
		i.stack = i.stack[:stackSize-2]
	case node.leftHash != nil || node.rightHash != nil:
		i.stack = i.stack[:stackSize-1]
	}
	i.stack = append(i.stack, node)

	return nil
}

// Commit finalizes the import by flushing any outstanding nodes to the database, making the
// version visible, and updating the tree metadata. It can only be called once, and calls Close()
// internally.
func (i *Importer) Commit() error {
	for len(i.chDataNode) > 0 || len(i.chNode) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println("[IAVL] Starting to commit")

	if i.tree == nil {
		return ErrNoImport
	}

	switch len(i.stack) {
	case 0:
		fmt.Println("[IAVL] Writing something for case 0")
		if err := i.batch.Set(i.tree.ndb.rootKey(i.version), []byte{}); err != nil {
			panic(err)
		}
	case 1:
		fmt.Println("[IAVL] Writing something for case 1")
		if err := i.batch.Set(i.tree.ndb.rootKey(i.version), i.stack[0].hash); err != nil {
			panic(err)
		}
	default:
		return errors.Errorf("invalid node structure, found stack size %v when committing",
			len(i.stack))
	}

	fmt.Println("[IAVL] Committing batch with write sync")
	err := i.batch.WriteSync()
	if err != nil {
		fmt.Printf("[IAVL] Committing batch hitting some err: %s", err.Error())
		return err
	}
	fmt.Println("[IAVL] Resetting latest version ")
	i.tree.ndb.resetLatestVersion(i.version)
	fmt.Println("[IAVL] Loading version ")
	_, err = i.tree.LoadVersion(i.version)
	fmt.Println("[IAVL] Loaded version ")
	if err != nil {
		return err
	}

	fmt.Println("[IAVL] Closing batch after commit()")
	i.Close()
	fmt.Println("[IAVL] Released lock in commit")
	return nil
}
