
Refactor API to split functionality into mutable and immutable inferfaces. This will enable read only snapshot interfaces to previously stored versions, and should significantly speed up load times for trees with many old versions. (Current code has nlog(n) overhead on load)

- TLDR: VersionedTree becomes MutableTree, Tree becomes ImmutableTree
- Merge VersionedTree and orphaningTree into MutableTree interface
- Move mutable methods on Tree into MutableTree
- Move recursive mutation methods on Node into MutableTree
- Move recursive lookup methods on Node into ImmutableTree
- MutableTree needs to include an ImmutableTree which it live replaces on update


`MutableTree` holds an `ImmutableTree` as a working tree, as well as the orphans map.
On commit, the tree is saved to disk as the next version, along with the orphans info.

On load, `MutableTree` just loads the most recent saved `ImmutableTree`.


## MutableTree
 - ImmutableTree*
 - Load()
 - Set(key, value)
 - Remove(key)
 - Commit()
 - Rollback()
 - GetImmutable(version)
 - DeleteImmutable(version)

## ImmutableTree
 - root *Node
 - Has(key)
 - Get(key)
 - IterateRange(start, end)
 - Hash()
 - Size()
 - Version()
 - GetWithProof(key)
 - GetRangeWithProof(start, end)
