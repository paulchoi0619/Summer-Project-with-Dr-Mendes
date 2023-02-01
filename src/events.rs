use super::*;
use bplus::{BPTree, Block, BlockId, Entry, InsertResult, Key};
use libp2p::core::PeerId;
use libp2p::request_response::ResponseChannel;
use network::{Client, GenericResponse};
use std::collections::{HashMap, HashSet};

pub async fn handle_lease_request(
    key: Key,
    entry: Entry,
    bp_tree: Arc<RwLock<BPTree>>,
    client: &mut Client,
    migrate_peer: PeerId,
    migrating_block: Arc<RwLock<HashSet<BlockId>>>,
    queries: Arc<RwLock<HashMap<BlockId, Vec<GeneralRequest>>>>,
    block_id: BlockId,
) {
    
    let current_id = {
        let bp_tree = bp_tree.read().unwrap();
        bp_tree.find(block_id, key) //read operation
    };
    
    let read_migrating_block = migrating_block.read().unwrap();
    
    if read_migrating_block.contains(&current_id) {
        let request = GeneralRequest::LeaseRequest(key, entry,current_id);
        let mut queries = queries.write().unwrap();
        if queries.contains_key(&current_id) {
            let requests = queries.get_mut(&current_id).unwrap();
            requests.push(request);
        } else {
            let requests = vec![request];
            queries.insert(current_id, requests);
        }
    } else {
        drop(read_migrating_block);
        let current_block = {
            let bp_tree = bp_tree.read().unwrap();
            bp_tree.get_block(current_id)
        }; //returns either the leaf or the internal block of the local b-plus tree
       
        if current_block.is_leaf() {
            
            if key >= current_block.return_divider_key() {
                //if the key does not belong in this leaf block
                let next_block_id = current_block.return_next_block();
                let request = GeneralRequest::LeaseRequest(key, entry,next_block_id);
                let peers = client.get_providers(next_block_id.to_string()).await;
                let _requests = peers.into_iter().map(|p| {
                    let mut network_client = client.clone();
                    let request = request.clone();
                    async move { network_client.request(p, request).await }.boxed()
                    //flush the query in the next block
                });
            } else {
                
                let mut write_bp_tree = bp_tree.write().unwrap();
                let result = write_bp_tree.insert(current_id, key, entry); //if the block is a leaf then add the entry (write operation)
                drop(write_bp_tree);
                match result {
                    InsertResult::Complete => {
                        //if the insertion is successful
                        println!("Success!");
                    }
                    //if it led to a split
                    InsertResult::RightBlock(block_id, divider_key) => {
                        let id = block_id;
                        let mut write_bp_tree = bp_tree.write().unwrap();
                        let block = write_bp_tree.get_block(id).clone();
                        let parent = block.parent();

                        //if the parent is in the local block map
                        if write_bp_tree.contains(parent) {
                            write_bp_tree.insert_child(divider_key, block_id, parent);
                            drop(write_bp_tree);
                        }
                        //else
                        else {
                            let divider_key_request = GeneralRequest::InsertOnRemoteParent(
                                divider_key,
                                block.parent(),
                                id,
                            );
                            let peers = client.get_providers(block.parent().to_string()).await;
                            let divider_key_requests = peers.into_iter().map(|p| {
                                let mut network_client = client.clone();
                                let divider_key_request = divider_key_request.clone();
                                async move { network_client.request(p, divider_key_request).await }
                                    .boxed()
                            });

                            let info = futures::future::select_ok(divider_key_requests).await;
                            match info {
                                Ok(str) => {
                                }
                                Err(err) => println!("Error {:?}", err),
                            };
                        }
                        let migrate_request = GeneralRequest::MigrateRequest(block);
                        let result = client.request(migrate_peer, migrate_request).await;
                        let mut write_migrating_block = migrating_block.write().unwrap();
                        write_migrating_block.insert(id);
                        drop(write_migrating_block);
                        println!("migrating");
                        //request migration
                        match result {
                            Ok(_) => {
                                println!("Completed migration");
                                let mut write_bp_tree = bp_tree.write().unwrap();
                                write_bp_tree.remove_block(id); //remove block from local b-plus tree
                                drop(write_bp_tree);
                                let mut write_migrating_block = migrating_block.write().unwrap();
                                write_migrating_block.remove(&id); //remove id from record set
                                drop(write_migrating_block);
                                
                                let q_records = queries.read().unwrap();
                                if q_records.contains_key(&id) {
                                    //if there are some queries that are needed to be flushed
                                    let mut queries = queries.write().unwrap();
                                    let pending_queries = queries.remove(&id).unwrap();
                                    drop(queries);
                                    for query in pending_queries {
                                        let result = client.request(migrate_peer, query).await;
                                        match result {
                                            Ok(str) => {}
                                            Err(err) => {
                                                println!("Error {:?}", err);
                                            }
                                        }
                                    }
                                }
                                drop(q_records);
                            }
                            Err(err) => {
                                println!("Error {:?}", err);
                            }
                        }
                    }
                }
            }
        }
        //the current peer does not contain the id
        else {
           
            let providers = client.get_providers(current_id.to_string()).await;
            println!("{:?}", providers);
            if providers.is_empty() {
                println!("Could not find provider for lease.");
            }
            let lease = GeneralRequest::LeaseRequest(key, entry,current_id); //send a lease request to the next peer
            let requests = providers.into_iter().map(|p| {
                let mut network_client = client.clone();
                let lease = lease.clone();
                async move { network_client.request(p, lease).await }.boxed()
            });
            let lease_info = futures::future::select_ok(requests).await;
            match lease_info {
                Ok(str) => {
                    println!("Here {:?}", str.0);
                }
                Err(err) => {
                    println!("Error {:?}", err);
                }
            };
        }
    }
    let bp_tree = bp_tree.read().unwrap();
    println!("{:?}", bp_tree.get_block_map());
}

pub async fn handle_insert_on_remote_parent(
    key: Key,
    parent: BlockId,
    child: BlockId,
    bp_tree: Arc<RwLock<BPTree>>,
    client: &mut Client,
    migrate_peer: PeerId,
    migrating_block: Arc<RwLock<HashSet<BlockId>>>,
    queries: Arc<RwLock<HashMap<BlockId, Vec<GeneralRequest>>>>,
) {
    let parent_block = {
        let bp_tree = bp_tree.read().unwrap();
        bp_tree.get_block(parent)
    };
    if key >= parent_block.return_divider_key() {
        //if the key does not belong to this parent block anymore
        let right_block = parent_block.return_next_block(); //adjacent block of the internal block
        let peer = client.get_providers(right_block.to_string()).await; //get the right block
        let next_call = GeneralRequest::InsertOnRemoteParent(key, right_block, child);
        let requests = peer.into_iter().map(|p| {
            let mut network_client = client.clone();
            let next_call = next_call.clone();
            async move { network_client.request(p, next_call).await }.boxed()
        });
        let insert_info = futures::future::select_ok(requests).await;
        match insert_info {
            Ok(str) => {
                let response: GeneralResponse = serde_json::from_str(&str.0).unwrap();
                match response {
                    GeneralResponse::InsertOnRemoteParent => {}
                    GeneralResponse::MigrateResponse => {}
                    GeneralResponse::LeaseResponse => {}
                }
            }
            Err(err) => {
                println!("Error {:?}", err);
            }
        };
    } else {
        let mut bp_tree = bp_tree.write().unwrap();
        let result = bp_tree.insert_child(key, child, parent);
        match result {
            //Successful insertion
            InsertResult::Complete => {}
            //Insertion led to split
            InsertResult::RightBlock(right_block_id, divider_key) => {
                //right block to split
                let block = bp_tree.get_block(right_block_id).clone();
                let migrate_request = GeneralRequest::MigrateRequest(block);
                let mut migrating_block = migrating_block.write().unwrap();
                migrating_block.insert(right_block_id);
                let result = client.request(migrate_peer, migrate_request).await; //request migration
                match result {
                    Ok(_) => {
                        bp_tree.remove_block(right_block_id); //remove block from local b-plus tree
                        drop(bp_tree);
                        migrating_block.remove(&right_block_id); //remove id from record set
                        let q_records = queries.read().unwrap();
                        if q_records.contains_key(&right_block_id) {
                            let mut queries = queries.write().unwrap();
                            let pending_queries = queries.remove(&right_block_id).unwrap();
                            drop(queries);
                            for query in pending_queries {
                                let result = client.request(migrate_peer, query).await;
                                match result {
                                    Ok(str) => {}
                                    Err(err) => {
                                        println!("Error {:?}", err);
                                    }
                                }
                            }
                        }
                        drop(q_records);
                    }
                    Err(err) => {
                        println!("Error {:?}", err);
                    }
                }
            }
        }
    }
    let bp_tree = bp_tree.read().unwrap();
    println!("{:?}", bp_tree.get_block_map());
}

pub async fn handle_migrate(block: Block, bp_tree: Arc<RwLock<BPTree>>, client: &mut Client) {
    let child_id = block.return_id();
    let mut write_bp_tree = bp_tree.write().unwrap();
    write_bp_tree.add_block(child_id, block);
    drop(write_bp_tree);
    client.start_providing(child_id.to_string()).await;
    let bp_tree = bp_tree.read().unwrap();
    println!("{:?}", bp_tree.get_block_map());
}
