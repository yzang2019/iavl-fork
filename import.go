package iavl

import (
	"bytes"
	"fmt"
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
	tree      *MutableTree
	version   int64
	batch     db.Batch
	batchSize uint32
	stack     []*Node
	chBatch   chan db.Batch
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
		tree:    tree,
		version: version,
		batch:   tree.ndb.db.NewBatch(),
		stack:   make([]*Node, 0, 8),
		chBatch: make(chan db.Batch, 1),
	}

	go periodicBatchCommit(importer)

	return importer, nil
}

func periodicBatchCommit(i *Importer) {
	for i.batch != nil {
		time.Sleep(200 * time.Millisecond)
		select {
		case nextBatch := <-i.chBatch:
			batchWriteStart := time.Now().UnixMicro()
			err := nextBatch.Write()
			if err != nil {
				panic(err)
			}
			nextBatch.Close()
			batchWriteEnd := time.Now().UnixMicro()
			batchCommitLatency := batchWriteEnd - batchWriteStart
			fmt.Printf("[IAVL IMPORTER] Batch commit latency: %d\n", batchCommitLatency/1000)
		default:
		}
	}
	fmt.Printf("[IAVL IMPORTER] Shutting down the batch commit thread\n")
}

// Close frees all resources. It is safe to call multiple times. Uncommitted nodes may already have
// been flushed to the database, but will not be visible.
func (i *Importer) Close() {
	if i.batch != nil {
		i.batch.Close()
	}
	i.batch = nil
	i.tree = nil
}

var totalPartA int64
var totalPartB int64
var totalPartC int64
var totalPartD int64
var totalBytes int64

// Add adds an ExportNode to the import. ExportNodes must be added in the order returned by
// Exporter, i.e. depth-first post-order (LRN). Nodes are periodically flushed to the database,
// but the imported version is not visible until Commit() is called.
func (i *Importer) Add(exportNode *ExportNode) error {

	partAStart := time.Now().UnixMicro()
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
	partBStart := time.Now().UnixMicro()
	totalPartA += partBStart - partAStart

	node._hash()
	err := node.validate()
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	err = node.writeBytes(&buf)
	if err != nil {
		return err
	}
	partBEnd := time.Now().UnixMicro()
	totalPartB += partBEnd - partBStart

	data := buf.Bytes()
	totalBytes += int64(len(data))
	if err = i.batch.Set(i.tree.ndb.nodeKey(node.hash), data); err != nil {
		return err
	}
	partCEnd := time.Now().UnixMicro()
	totalPartC += partCEnd - partBEnd

	i.batchSize++
	if i.batchSize >= maxBatchSize && len(i.chBatch) <= 0 {
		fmt.Printf("[IAVL IMPORTER] Flushing a batch with batch size %d, items %d, stack size %d \n", totalBytes, i.batchSize, len(i.stack))
		i.chBatch <- i.batch
		i.batch = i.tree.ndb.db.NewBatch()
		i.batchSize = 0
		fmt.Printf("[IAVL IMPORTER] Total part A latency: %d, total part B latency: %d, total part C latency: %d, total part D latency: %d\n", totalPartA/1000, totalPartB/1000, totalPartC/1000, totalPartD/1000)
		totalPartA = 0
		totalPartB = 0
		totalPartC = 0
		totalPartD = 0
		totalBytes = 0
	}

	partDStart := time.Now().UnixMicro()
	// Update the stack now that we know there were no errors
	switch {
	case node.leftHash != nil && node.rightHash != nil:
		i.stack = i.stack[:stackSize-2]
	case node.leftHash != nil || node.rightHash != nil:
		i.stack = i.stack[:stackSize-1]
	}
	i.stack = append(i.stack, node)
	partDEnd := time.Now().UnixMicro()
	totalPartD += partDEnd - partDStart
	return nil
}

// Commit finalizes the import by flushing any outstanding nodes to the database, making the
// version visible, and updating the tree metadata. It can only be called once, and calls Close()
// internally.
func (i *Importer) Commit() error {
	if i.tree == nil {
		return ErrNoImport
	}

	switch len(i.stack) {
	case 0:
		if err := i.batch.Set(i.tree.ndb.rootKey(i.version), []byte{}); err != nil {
			panic(err)
		}
	case 1:
		if err := i.batch.Set(i.tree.ndb.rootKey(i.version), i.stack[0].hash); err != nil {
			panic(err)
		}
	default:
		return errors.Errorf("invalid node structure, found stack size %v when committing",
			len(i.stack))
	}

	err := i.batch.WriteSync()
	if err != nil {
		return err
	}
	i.tree.ndb.resetLatestVersion(i.version)

	_, err = i.tree.LoadVersion(i.version)
	if err != nil {
		return err
	}

	i.Close()
	return nil
}
