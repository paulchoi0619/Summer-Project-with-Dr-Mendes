use libp2p::core::PeerId;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

pub struct BPTree {
    block_map: HashMap<Key, Block>,
    top_id: BlockId,
}
impl BPTree {
    pub fn new() -> Self {
        let mut map = HashMap::new();
        // let id = root.block_id.clone();
        // map.insert(root.block_id, root);

        Self {
            block_map: map,
            top_id: Default::default(),
        }
    }
    pub fn get_size(&self) -> usize {
        self.block_map.keys().len()
    }
    pub fn remove_block(&mut self, id: BlockId) {
        self.block_map.remove(&id);
    }
    pub fn add_block(&mut self, id: BlockId, block: Block) {
        self.block_map.insert(id, block);
    }
    pub fn get_block_map(&self) -> &HashMap<Key, Block> {
        &self.block_map
    }
    pub fn get_top_id(&self) -> BlockId {
        self.top_id
    }
    pub fn set_top_id(&mut self, id: BlockId) {
        self.top_id = id;
    }
    pub fn get_block(&mut self, id: BlockId) -> &mut Block {
        self.block_map.get_mut(&id).unwrap()
    }
    pub fn find(&self, block_id: BlockId, k: Key) -> BlockId {
        //read operation
        let current = self.block_map.get(&block_id).unwrap();
        if !current.is_leaf {
            for i in 0..current.keys.len() {
                if k <= current.keys[i] {
                    let new_id = current.children[i];
                    if self.block_map.contains_key(&new_id) {
                        return self.find(new_id, k); //if the child id is in block map, do a recursive search
                    } else {
                        return block_id; //else return this id since this local block map does not contain the block
                    }
                }
            }
            let new_id = current.children[current.keys.len() - 1]; //retreive the last child id
            if self.block_map.contains_key(&new_id) {
                return self.find(new_id, k); //if the child is in block map, continue with search
            } else {
                return block_id; //else return the internal block id
            }
        }
        return block_id; //return the leaf block id
    }
    pub fn insert_child(
        &mut self,
        key: Key,
        child: BlockId,
        current_block: BlockId,
    ) -> InsertResult {
        let current_block = self.get_block(current_block);
        current_block.add_child(key, child);
        if current_block.keys.len() == SIZE {
            let mut new_block = current_block.clone(); //create new block to split and update block map
            let result = new_block.split_internal_block(&mut self.block_map);
            return InsertResult::RightBlock(result.right);
        } else {
            return InsertResult::Complete;
        }
    }

    pub fn insert(&mut self, leaf_id: BlockId, key: Key, entry: Entry) -> InsertResult {
        let leaf = self.block_map.get_mut(&leaf_id).unwrap();
        leaf.add_entry(key, entry);

        if leaf.keys.len() == SIZE {
            let mut newleaf = leaf.clone(); //create a new leaf to split and update block map
            if leaf.parent() == 0 {
                //checking if this is a root
                let result = newleaf.split_leaf_root(&mut self.block_map);
                self.block_map
                    .get_mut(&result.left)
                    .unwrap()
                    .set_next_block(result.right); //create a link between left and right nodes
                return InsertResult::RightBlock(result.right);
            }
            let result = newleaf.split_leaf_block(&mut self.block_map); //splits the block and adds them to the block map
            self.block_map
                .get_mut(&result.left)
                .unwrap()
                .set_next_block(result.right); //create a link between left and right nodes
            return InsertResult::RightBlock(result.right);
        }
        return InsertResult::Complete;
    }
}

pub const SIZE: usize = 5;

pub type BlockId = u64;

pub type Key = u64; //
#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct Block {
    parent: BlockId,
    block_id: BlockId,
    keys: Vec<Key>,
    children: Vec<BlockId>,
    values: Vec<Entry>,
    is_leaf: bool,
    divider_key: Key,
    next_block: BlockId,
}
impl Block {
    // creates a fresh block
    pub fn new() -> Self {
        Self {
            block_id: Default::default(),
            parent: 0, //default parent
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            is_leaf: true,
            divider_key: std::u64::MAX,
            next_block: Default::default(),
        }
    }
    pub fn set_next_block(&mut self, next_block: BlockId) {
        self.next_block = next_block;
    }
    pub fn return_next_block(&self) -> BlockId {
        self.next_block
    }
    pub fn return_divider_key(&self) -> Key {
        self.divider_key
    }
    pub fn return_parent_key(&self) -> Key {
        self.keys[0]
    }
    pub fn return_id(&self) -> BlockId {
        self.block_id
    }
    pub fn parent(&self) -> BlockId {
        self.parent
    }
    pub fn is_leaf(&self) -> bool {
        self.is_leaf
    }
    pub fn set_block_id(&mut self) {
        let id = rand::thread_rng().gen_range(1, std::u64::MAX);
        self.block_id = id;
    }
    pub fn set_parent(&mut self, parent: BlockId) {
        self.parent = parent;
    }
    pub fn add_entry(&mut self, k: Key, new_entry: Entry) {
        for i in 0..self.keys.len() {
            if self.keys[i] > k {
                self.keys.insert(i, k);
                self.values.insert(i, new_entry);
                return;
            }
        }
        self.keys.push(k);
        self.values.push(new_entry);
    }
    pub fn add_child(&mut self, k: Key, new_block: BlockId) {
        for i in 0..self.keys.len() {
            if self.keys[i] == k {
                self.keys.insert(i, k);
                self.children.insert(i + 1, new_block); //insert child
                return;
            }
        }
        self.keys.push(k);
        self.children.push(new_block);
    }

    pub fn split_leaf_root(&mut self, block_map: &mut HashMap<u64, Block>) -> SplitResult {
        let mut result = SplitResult::new(self.block_id);
        let mut new_root = Block::new();
        new_root.set_block_id();
        new_root.is_leaf = false;

        let mut leftblock = block_map.get(&result.left).unwrap().clone();
        leftblock.parent = new_root.block_id; //update parent of left block

        new_root.children.push(leftblock.block_id); //add child

        let mut rightblock = Block::new();
        rightblock.set_block_id();
        rightblock.parent = new_root.block_id; //update potential parent of right block
        result.right = rightblock.block_id;
        let counter: usize = SIZE / 2;
        let length = leftblock.keys.len().clone();
        for i in counter..length {
            let entry = leftblock.values[i].clone();
            rightblock.add_entry(leftblock.keys[i], entry);
        }
        for _ in counter..length {
            leftblock.keys.pop();
            leftblock.values.pop();
        }
        new_root.keys.push(rightblock.keys[0]);
        result.divider_key = rightblock.keys[0];
        leftblock.divider_key = result.divider_key; //sets the max range for leftblock
        block_map.insert(new_root.block_id, new_root); // add new root to block map
        block_map.insert(rightblock.block_id, rightblock); // add right block
        block_map.insert(leftblock.block_id, leftblock); // add left block
        return result;
    }

    pub fn split_leaf_block(&mut self, block_map: &mut HashMap<u64, Block>) -> SplitResult {
        let mut result = SplitResult::new(self.block_id);
        let mut leftblock = block_map.get(&result.left).unwrap().clone();
        let mut rightblock = Block::new();
        rightblock.set_block_id(); //set block id for right block
        result.right = rightblock.block_id;
        rightblock.parent = leftblock.parent; //put right block's potential parent as left block's parent
        let counter: usize = SIZE / 2;
        let length = leftblock.keys.len().clone();
        for i in counter..length {
            let entry = leftblock.values[i].clone();
            rightblock.add_entry(leftblock.keys[i], entry);
        }
        for _ in counter..length {
            leftblock.keys.pop();
            leftblock.values.pop();
        }
        result.divider_key = rightblock.keys[0];
        leftblock.divider_key = result.divider_key; //sets the max range for leftblock
        block_map.insert(rightblock.block_id, rightblock); //insert into map
        block_map.insert(leftblock.block_id, leftblock); //update
        return result;
    }

    pub fn split_internal_block(&mut self, block_map: &mut HashMap<u64, Block>) -> SplitResult {
        let mut result = SplitResult::new(self.block_id);
        let mut leftblock = block_map.get(&self.block_id).unwrap().clone();
        let mut rightblock = Block::new();

        rightblock.set_block_id();
        result.right = rightblock.block_id;
        rightblock.parent = leftblock.parent;
        rightblock.is_leaf = false;

        let counter: usize = 1 + SIZE / 2;
        let length = leftblock.keys.len().clone();
        for i in counter..length {
            rightblock.add_child(leftblock.keys[i], leftblock.children[i]);
        }
        for _ in counter..length {
            leftblock.keys.pop();
            leftblock.children.pop();
        }
        result.divider_key = leftblock.keys.pop().unwrap();
        leftblock.divider_key = result.divider_key; //sets the max range for leftblock
        block_map.insert(rightblock.block_id, rightblock); //insert into map
        block_map.insert(leftblock.block_id, leftblock); //update
        return result;
    }
}

pub enum InsertResult {
    Complete,
    RightBlock(BlockId),
}

pub struct SplitResult {
    left: BlockId,
    right: BlockId,
    divider_key: Key,
}
impl SplitResult {
    fn new(left: BlockId) -> SplitResult {
        SplitResult {
            left: left,
            right: Default::default(),
            divider_key: Default::default(),
        }
    }
}
#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct Data;
impl Data {
    fn empty() -> Self {
        Self
    }
}
#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub struct Entry {
    myid: PeerId,
    key: Key,
    data: Data,
}
impl Entry {
    pub fn new(id: PeerId, key: Key) -> Self {
        Self {
            myid: id,
            key,
            data: Data::empty(),
        }
    }
}
impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.myid == other.myid
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.myid.partial_cmp(&other.myid)
    }
}
