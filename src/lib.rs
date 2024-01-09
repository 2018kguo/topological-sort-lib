#![allow(dead_code)]

use anyhow::Result;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};

// Implementation goals:
// lookup all parents quickly for each dep
// when marking a node as visited, mark all children as visited quickly
// get the nodes to visit next quickly

// struct Prepared;
// struct Unprepared;

#[derive(PartialEq)]
enum PreparedState {
    Prepared,
    Unprepared,
}

#[derive(Default)]
enum ParentDependencyType {
    #[default]
    Any,
    All, // only visit when all parents are visited
}

#[derive(PartialEq)]
enum NodeStatus {
    Pending,
    Collected,
    Visited,
}

// TODO use this later
// pub enum GraphConstraint {
//     SingleParentOnly,
//     MultipleParentsAllowed,
//     IsolatedNodesAllowed,
// }

// TODO: maybe make this generic over the hash type, would probably need a derive macro
// TODO: store references to the nodes instead of the nodes themselves?
// TODO: use type state for the prepared/unprepared state
struct TopologicalSorter<NodeType> {
    map: HashMap<u64, NodeType>,
    ready_node_hashes: HashSet<u64>,
    node_statuses: HashMap<u64, NodeStatus>,
    parent_dependencies: HashMap<u64, HashSet<u64>>,
    child_dependencies: HashMap<u64, HashSet<u64>>,
    node_parent_dependency_types: HashMap<u64, ParentDependencyType>,
    prepared_state: PreparedState,
}

#[derive(Debug)]
enum VisitError {
    SorterNotPrepared,
    NodeNotAdded,
    NodeNotReady,
    NodeAlreadyVisited,
}

impl fmt::Display for VisitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VisitError::SorterNotPrepared => write!(f, "Sorter not prepared"),
            VisitError::NodeNotAdded => write!(f, "Node not added"),
            VisitError::NodeNotReady => write!(f, "Node not ready"),
            VisitError::NodeAlreadyVisited => write!(f, "Node already visited"),
        }
    }
}

impl std::error::Error for VisitError {}

impl<T> TopologicalSorter<T>
where
    T: Hash + Eq,
{
    // constructor
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            ready_node_hashes: HashSet::new(),
            node_statuses: HashMap::new(),
            parent_dependencies: HashMap::new(),
            child_dependencies: HashMap::new(),
            node_parent_dependency_types: HashMap::new(),
            prepared_state: PreparedState::Unprepared,
        }
    }

    // insert a node into the map, return the hash of the node
    fn insert(&mut self, node: T, parent_dependency_type: ParentDependencyType) -> Result<u64> {
        if self.prepared_state == PreparedState::Prepared {
            return Err(anyhow::anyhow!("Already prepared"));
        }
        let hash = Self::hash(self, &node);
        self.map.insert(hash, node);
        self.node_parent_dependency_types
            .insert(hash, parent_dependency_type);
        self.node_statuses.insert(hash, NodeStatus::Pending);
        Ok(hash)
    }

    fn add_dependencies(
        &mut self,
        parent_node_hash: u64,
        child_node_hashes: &Vec<u64>,
    ) -> Result<()> {
        if self.prepared_state == PreparedState::Prepared {
            return Err(anyhow::anyhow!("Already prepared"));
        }

        // add the parent node hash to the child dependencies
        let child_dependencies = self
            .child_dependencies
            .entry(parent_node_hash)
            .or_insert(HashSet::new());
        for &child_node_hash in child_node_hashes {
            child_dependencies.insert(child_node_hash);
        }

        // add the parent to the child dependencies
        for &child_node_hash in child_node_hashes {
            let parent_dependencies = self
                .parent_dependencies
                .entry(child_node_hash)
                .or_insert(HashSet::new());
            parent_dependencies.insert(parent_node_hash);
        }
        Ok(())
    }

    // get the hash of a node
    fn hash(&mut self, node: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        node.hash(&mut hasher);
        let hash_value = hasher.finish();
        hash_value
    }

    fn prepare(&mut self) -> Result<()> {
        match self.prepare_impl() {
            Ok(_) => {
                self.prepared_state = PreparedState::Prepared;
                // get all nodes that are in map but are not in parent_dependencies's keys
                self.ready_node_hashes = self
                    .map
                    .keys()
                    .filter(|&hash| !self.parent_dependencies.contains_key(hash))
                    .cloned()
                    .collect();
                Ok(())
            }
            Err(e) => {
                self.prepared_state = PreparedState::Unprepared;
                Err(e)
            }
        }
    }

    fn prepare_impl(&mut self) -> Result<()> {
        // TODO: update this validation based on graph constraints later
        // if there are multiple nodes with no parents, then error
        // if self.parent_dependencies.len() != (self.map.len() - 1) {
        //     return Err(anyhow::anyhow!("There are multiple nodes with no parents"));
        // }

        // if there are cycles, then error
        Self::find_cycle(&self.parent_dependencies, &self.child_dependencies)?;
        Ok(())
    }

    fn find_cycle(
        parent_dependencies: &HashMap<u64, HashSet<u64>>,
        child_dependencies: &HashMap<u64, HashSet<u64>>,
    ) -> Result<()> {
        // check if any intersection between parent and child dependencies
        // if there is, then there is a cycle

        // Flatten the parent dependencies into a single HashSet
        let mut all_parent_hashes = HashSet::new();
        for hashes in parent_dependencies.values() {
            for hash in hashes {
                all_parent_hashes.insert(*hash);
            }
        }

        // Check if any child dependency intersects with the parent hashes
        for hashes in child_dependencies.values() {
            if hashes.intersection(&all_parent_hashes).next().is_some() {
                // Found an intersection, which indicates a cycle
                return Err(anyhow::anyhow!("Cycle detected between two nodes"));
            }
        }

        Ok(())
    }

    fn peek_ready_nodes(&self) -> Result<Vec<&T>> {
        if !(self.prepared_state == PreparedState::Prepared) {
            return Err(anyhow::anyhow!("Not prepared"));
        }
        let mut ready_nodes = Vec::new();
        for &hash in &self.ready_node_hashes {
            let node = self.map.get(&hash).unwrap();
            ready_nodes.push(node);
        }
        Ok(ready_nodes)
    }

    fn collect_ready_nodes(&mut self) -> Result<Vec<T>> {
        if !(self.prepared_state == PreparedState::Prepared) {
            return Err(anyhow::anyhow!("Not prepared"));
        }
        let mut ready_nodes = Vec::new();
        for &hash in &self.ready_node_hashes {
            let node = self.map.remove(&hash).unwrap();
            ready_nodes.push(node);
            self.node_statuses.insert(hash, NodeStatus::Collected);
        }
        self.ready_node_hashes.clear();
        Ok(ready_nodes)
    }

    fn mark_node_as_visited(&mut self, node: &T) -> std::result::Result<u64, VisitError> {
        if !(self.prepared_state == PreparedState::Prepared) {
            return Err(VisitError::SorterNotPrepared);
        }
        let hash = Self::hash(self, &node);
        if !self.node_statuses.contains_key(&hash) {
            return Err(VisitError::NodeNotAdded);
        }
        if !self.ready_node_hashes.contains(&hash)
            && !(self.node_statuses.get(&hash).unwrap() == &NodeStatus::Collected)
        {
            return Err(VisitError::NodeNotReady);
        }
        if self.node_statuses.get(&hash).unwrap() == &NodeStatus::Visited {
            return Err(VisitError::NodeAlreadyVisited);
        }

        self.node_statuses.insert(hash, NodeStatus::Visited);
        let children_hashes = self.child_dependencies.get(&hash).unwrap();
        for child_hash in children_hashes {
            let parent_dep_type = self.node_parent_dependency_types.get(&child_hash).unwrap();
            match parent_dep_type {
                ParentDependencyType::Any => {
                    self.ready_node_hashes.insert(*child_hash);
                }
                ParentDependencyType::All => {
                    // check if all parents are visited
                    match self.parent_dependencies.get(&child_hash) {
                        Some(parent_hashes) => {
                            let all_parent_hashes_visited =
                                parent_hashes.iter().all(|&parent_hash| {
                                    self.node_statuses.get(&parent_hash).unwrap()
                                        == &NodeStatus::Visited
                                });
                            if all_parent_hashes_visited {
                                self.ready_node_hashes.insert(*child_hash);
                            }
                        }
                        None => {
                            // this node has no parents, so it is ready to be visited
                            self.ready_node_hashes.insert(*child_hash);
                        }
                    }
                }
            }
        }

        Ok(hash)
    }

    fn is_finished(&self) -> bool {
        self.prepared_state == PreparedState::Prepared && self.ready_node_hashes.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::ParentDependencyType;
    use super::TopologicalSorter;
    use std::error::Error;
    type TestResult = Result<(), Box<dyn Error>>;

    #[test]
    fn test_insert() -> TestResult {
        let mut sorter = TopologicalSorter::new();
        sorter.insert("hello", ParentDependencyType::default())?;
        assert_eq!(sorter.map.len(), 1);
        Ok(())
    }

    #[test]
    fn test_prepare_simple() -> TestResult {
        let mut sorter = TopologicalSorter::new();
        sorter.insert("hello", ParentDependencyType::default())?;
        let result = sorter.prepare();
        assert!(result.is_ok());

        assert!(sorter.prepared_state == super::PreparedState::Prepared);
        assert!(sorter.ready_node_hashes.len() == 1);
        let hash = sorter.hash(&"hello");
        assert!(sorter.ready_node_hashes.contains(&hash));
        Ok(())
    }

    #[test]
    fn test_prepare_multiple_nodes() -> TestResult {
        let mut sorter = TopologicalSorter::new();
        sorter.insert("hello", ParentDependencyType::default())?;
        sorter.insert("world", ParentDependencyType::default())?;
        let result = sorter.prepare();
        assert!(result.is_ok());

        assert!(sorter.prepared_state == super::PreparedState::Prepared);
        assert!(sorter.ready_node_hashes.len() == 2);
        Ok(())
    }

    #[test]
    fn test_add_dependencies() -> TestResult {
        let mut sorter = TopologicalSorter::new();
        let hash = sorter.insert("hello", ParentDependencyType::default())?;
        let child_hash = sorter.insert("world", ParentDependencyType::default())?;

        sorter.add_dependencies(hash, &vec![child_hash])?;
        assert!(sorter.parent_dependencies.len() == 1);
        assert!(sorter.child_dependencies.len() == 1);

        println!("parent_dependencies: {:?}", sorter.parent_dependencies);
        assert!(sorter
            .parent_dependencies
            .get(&child_hash)
            .unwrap()
            .contains(&hash));
        assert!(sorter
            .child_dependencies
            .get(&hash)
            .unwrap()
            .contains(&child_hash));
        Ok(())
    }

    #[test]
    fn test_find_cycle() -> TestResult {
        let mut sorter = TopologicalSorter::new();
        let hash = sorter.insert("hello", ParentDependencyType::default())?;
        let child_hash = sorter.insert("world", ParentDependencyType::default())?;

        sorter.add_dependencies(hash, &vec![child_hash])?;
        sorter.add_dependencies(child_hash, &vec![hash])?;

        let result = sorter.prepare();
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_collect_and_peek_ready_nodes() -> TestResult {
        let mut sorter = TopologicalSorter::new();
        let hash = sorter.insert("hello", ParentDependencyType::default())?;
        let child_hash = sorter.insert("world", ParentDependencyType::default())?;

        sorter.add_dependencies(hash, &vec![child_hash])?;

        sorter.prepare()?;
        let peeked_ready_nodes = sorter.peek_ready_nodes()?;
        assert!(peeked_ready_nodes.len() == 1);
        let peeked_node = peeked_ready_nodes[0];
        assert!(peeked_node == &"hello");

        let ready_nodes = sorter.collect_ready_nodes()?;

        assert!(ready_nodes.len() == 1);
        assert!(ready_nodes[0] == "hello");
        Ok(())
    }

    #[test]
    fn test_collect_and_mark_as_visited_happy_path() -> TestResult {
        let mut sorter = TopologicalSorter::new();
        let hash = sorter.insert("hello", ParentDependencyType::default())?;
        let child_hash = sorter.insert("world", ParentDependencyType::default())?;

        sorter.add_dependencies(hash, &vec![child_hash])?;

        sorter.prepare()?;
        let ready_nodes = sorter.collect_ready_nodes()?;
        assert!(ready_nodes.len() == 1);
        let node = &ready_nodes[0];
        sorter.mark_node_as_visited(node)?;
        assert!(!sorter.is_finished());

        let ready_nodes = sorter.collect_ready_nodes()?;
        assert!(ready_nodes.len() == 1);
        assert!(ready_nodes[0] == "world");

        let ready_nodes = sorter.collect_ready_nodes()?;
        assert!(ready_nodes.is_empty());

        assert!(sorter.is_finished());
        Ok(())
    }

    #[test]
    fn test_any_vs_all_parent_dependencies() -> TestResult {
        let mut sorter = TopologicalSorter::new();
        let parent_1 = sorter.insert("hello", ParentDependencyType::Any)?;
        let parent_2 = sorter.insert("world", ParentDependencyType::All)?;
        let any_parent_dep_child = sorter.insert("any_parent", ParentDependencyType::Any)?;
        let all_parent_dep_child = sorter.insert("all_parent", ParentDependencyType::All)?;

        sorter.add_dependencies(parent_1, &vec![all_parent_dep_child, any_parent_dep_child])?;
        sorter.add_dependencies(parent_2, &vec![all_parent_dep_child, any_parent_dep_child])?;

        sorter.prepare()?;
        let ready_nodes = sorter.collect_ready_nodes()?;
        assert!(ready_nodes.len() == 2);

        sorter.mark_node_as_visited(&ready_nodes[0])?;
        let peeked_nodes = sorter.peek_ready_nodes()?;
        assert!(peeked_nodes.len() == 1);
        assert!(peeked_nodes[0] == &"any_parent");

        sorter.mark_node_as_visited(&ready_nodes[1])?;
        let peeked_nodes = sorter.peek_ready_nodes()?;
        assert!(peeked_nodes.len() == 2);
        assert!(peeked_nodes.contains(&&"any_parent"));
        assert!(peeked_nodes.contains(&&"all_parent"));
        Ok(())
    }
}
