
use tokio;
use libp2p::identity;
use serde::{Serialize, Deserialize};
use serde_json;
use libp2p::core::{PeerId, either};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::default;
use std::hash::Hasher;
use rand::Rng;
use futures::future::Either;
pub struct BPTree{
    block_map: HashMap<Key,Block>,
    top_id: BlockId
}
impl BPTree{

pub fn new (root:Block) -> Self{
    let mut map = HashMap::new();
    let id = root.block_id.clone();
    map.insert(root.block_id,root);
    Self{
    block_map: map,
    top_id: id
    }
}

pub fn get_top_id(&self) -> BlockId{
    self.top_id
}

pub fn get_block(&mut self,id:BlockId) -> &mut Block{
    self.block_map.get_mut(&id).unwrap()
}

pub fn find(&self, block_id:BlockId,k:Key)->BlockId{
    let current = self.block_map.get(&block_id).unwrap();
    if !current.is_leaf{
        for i in 0..current.keys.len(){
            if k<=current.keys[i]{
                let new_id = current.children[i];
                if self.block_map.contains_key(&new_id){
                return self.find(new_id,k);}
                else{
                    return new_id;
                }
            }
        }
        let new_id = current.children[current.keys.len()-1];
        if self.block_map.contains_key(&new_id){
            return self.find(new_id,k);}
        else{
            return new_id;
        }
    }
    return block_id;
    
}
pub fn contains_block(&mut self,block_id:BlockId) -> bool{
    self.block_map.contains_key(&block_id)
}

pub fn insert_child(&mut self, key:Key, child:BlockId) ->InsertResult{
    let current_id = self.find(self.top_id,key);
    let current_block = self.get_block(current_id);
    current_block.add_child(key,child);
    if current_block.keys.len() == SIZE{
        let mut new_block = current_block.clone(); //create new block to split and update block map
        let result = new_block.split_internal_block(&mut self.block_map);
        return self.insert_on_parent(result.left,result.divider_key,result.right);
    }
    else{
        return InsertResult::Complete;
    }
}

pub fn insert(&mut self,leaf_id:BlockId,key:Key,entry:Entry)->InsertResult{
    let leaf = self.block_map.get_mut(&leaf_id).unwrap();
    leaf.add_entry(key, entry);
  
    if leaf.keys.len() ==SIZE{
        let mut newleaf= leaf.clone(); //create a new leaf to split and update block map
        let result = newleaf.split_leaf_block(&mut self.block_map);
          
        return self.insert_on_parent(result.left,result.divider_key,result.right);
        
    }
    InsertResult::Complete
}
pub fn insert_on_parent(&mut self,left:BlockId,key:Key,right:BlockId)->InsertResult{
    let mut rightblock = self.block_map.get(&right).unwrap().clone(); //retrieve right block from the block map if it exists
    let mut leftblock = self.block_map.get_mut(&left).unwrap().clone(); //retrieve left block from the block map
    if left== self.top_id{
        
        let mut new_root =  Block::new(); 
        new_root.set_block_id();
        self.top_id = new_root.block_id;
        new_root.is_leaf=false;
        new_root.children.push(left);
        new_root.add_child(key,right);
       
        rightblock.parent = new_root.block_id; //update parent of right block
        leftblock.parent = new_root.block_id; //update parent of left block
        self.top_id = new_root.block_id;
        self.block_map.insert(new_root.block_id,new_root);// add new root to block map
        self.block_map.insert(rightblock.block_id,rightblock); // insert/update right block
        self.block_map.insert(leftblock.block_id,leftblock); // update left block
        return InsertResult::Complete;
    }
    else{
        rightblock.parent = leftblock.parent;
        if !self.block_map.contains_key(&leftblock.parent){
            
            return InsertResult::InsertOnRemoteParent(leftblock.parent,key,right);

        }
        let mut parent = self.block_map.get_mut(&leftblock.parent).unwrap().clone(); //retrieve parent block
        parent.add_child(key,right);  //add right block to the parent
        self.block_map.insert(rightblock.block_id,rightblock); //update right block in map
        
        if parent.keys.len()== SIZE{
        let result = parent.split_internal_block(&mut self.block_map);
        return self.insert_on_parent(result.left,result.divider_key,result.right);
        }
        else{
            self.block_map.insert(parent.block_id,parent); // update parent in the map 
            return InsertResult::Complete;
        }
       
    
    }
}


}


pub const SIZE: usize = 5;

pub type BlockId = u64;

pub type Key = u64; // 
#[derive(Debug,Serialize,Deserialize,Clone,Hash)]
pub struct Block{ 
    parent:BlockId,
    block_id: BlockId, 
    keys: Vec<Key>, 
    children: Vec<BlockId>,
    values: Vec<Entry>,
    is_leaf: bool,
}
impl Block{
    // creates a fresh block
    pub fn new () -> Self {


        // we would hash the peerid of the peer we migrate the block to for the remote version
        
        Self { 
            block_id: Default::default(),
            parent: Default::default(),
            keys: Vec::new(), 
            children: Vec::new(),
            values: Vec::new(),
            is_leaf: true,
        }
    }
    pub fn return_id(&mut self) -> BlockId{
        self.block_id
    }
    pub fn parent(&mut self) -> BlockId{
        self.parent
    }
    pub fn is_leaf(&mut self) -> bool{
        self.is_leaf
    }
   pub fn set_block_id(&mut self){
        let id = rand::thread_rng().gen_range(1,10000);
        self.block_id= id;
    }
    pub fn add_entry(&mut self, k:Key, new_entry: Entry){
        for i in 0..self.keys.len(){
            if self.keys[i]>k{
                self.keys.insert(i,k);
                self.values.insert(i,new_entry);
                return;
            }
        }
        self.keys.push(k);
        self.values.push(new_entry);

    }
    pub fn add_child(&mut self, k:Key, new_block: BlockId){
        for i in 0..self.keys.len(){
            if self.keys[i]>k{
                self.keys.insert(i,k);
                self.children.insert(i+1,new_block);
                return;
            }
        }
        self.keys.push(k);
        self.children.push(new_block);



    }


    // return 2 blocks
    pub fn split_leaf_block(&mut self, block_map:&mut HashMap<u64,Block>) -> SplitResult{
        let mut result = SplitResult::new(self.block_id);
        let mut leftblock = block_map.get(&result.left).unwrap().clone();
        let mut rightblock = Block::new();
        rightblock.set_block_id();
        result.right = rightblock.block_id;
        rightblock.parent = leftblock.parent; 
        let counter:usize = SIZE/2;
        let length = leftblock.keys.len().clone();
        for i in counter..length{
            let entry = leftblock.values[i].clone();
            rightblock.add_entry(leftblock.keys[i],entry)
        }
        for _ in counter..length{
            leftblock.keys.pop();
            leftblock.values.pop();
        }
        result.divider_key = rightblock.keys[0];
        block_map.insert(rightblock.block_id,rightblock); //insert into map 
        block_map.insert(leftblock.block_id,leftblock); //update 
        return result;


    }
    // return 2 blocks
    pub fn split_internal_block(&mut self, block_map:&mut HashMap<u64,Block>) -> SplitResult{
        let mut result = SplitResult::new(self.block_id);
        let mut leftblock = block_map.get(&result.left).unwrap().clone();
        let mut rightblock = Block::new();
        rightblock.set_block_id();
        result.right = rightblock.block_id;
        rightblock.parent = leftblock.parent;
        rightblock.is_leaf=false;
        let counter:usize = 1+SIZE/2;
        let length = leftblock.keys.len().clone();
        for i in counter..length{
            rightblock.add_child(leftblock.keys[i],leftblock.children[i]);
        }
        for _ in counter..length{
            leftblock.keys.pop();
            leftblock.children.pop();
        }
        result.divider_key = leftblock.keys.pop().unwrap();
        block_map.insert(rightblock.block_id,rightblock); //insert into map
        block_map.insert(leftblock.block_id,leftblock); //update 
        return result;
    }

}

pub enum InsertResult{
    Complete,
    InsertOnRemoteParent(BlockId,Key,BlockId),
}
pub struct SplitResult{
    left: BlockId,
    right: BlockId,
    divider_key: Key
}
impl SplitResult{
    fn new(left:BlockId)->SplitResult{

        SplitResult{left:left,right: Default::default(), divider_key:0 //default 
        }
    }
}
#[derive(Debug,Serialize,Deserialize,Clone,Hash)]
pub struct Data;
impl Data{
    fn empty() -> Self{
        Self
    }
}
#[derive(Debug,Serialize,Deserialize,Clone,Hash)]
pub struct Entry{
    myid: PeerId,
    key: Key,
    data: Data,
}
impl Entry{
    pub fn new(id:PeerId,key:Key) -> Self{
        Self { myid: id, key, data: Data::empty()}
    }
}
impl PartialEq for Entry{
    fn eq(&self, other: &Self) -> bool {
        self.myid == other.myid
    }
}
impl PartialOrd for Entry{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.myid.partial_cmp(&other.myid)
    }
}

