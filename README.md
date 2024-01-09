# topological-sort-lib

### Example usage:

```
let mut sorter = TopologicalSorter::new();

let hash = sorter.insert("hello", ParentDependencyType::default())?;
let child_hash = sorter.insert("world", ParentDependencyType::default())?;
sorter.add_dependencies(hash, &vec![child_hash])?;
sorter.prepare()?;

let ready_nodes = sorter.collect_ready_nodes()?;
let node = &ready_nodes[0];
assert!(ready_nodes[0] == "hello");
sorter.mark_node_as_visited(node)?;

let ready_nodes = sorter.collect_ready_nodes()?;
assert!(ready_nodes[0] == "world");

assert!(sorter.is_finished());
```