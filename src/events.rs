use super::*;
use bplus::{BPTree, Block, BlockId, Entry, InsertResult, Key};
use libp2p::core::PeerId;
use libp2p::request_response::ResponseChannel;
use network::{Client, GenericResponse};
use std::collections::{HashMap, HashSet};

pub async fn handle_lease_request(
    key: Key,
    entry: Entry,
    bp_tree: &RwLock<BPTree>,
    client: &mut Client,
    migrate_peer: &PeerId,
    migrating_block: &mut HashSet<BlockId>,
    queries: &mut HashMap<BlockId, Vec<GeneralRequest>>,
) {
    let current_id = {
    let bp_tree = bp_tree.read().unwrap();
    let top_id = bp_tree.get_top_id(); //the topmost block id of the local b-plus tree
    bp_tree.find(top_id, key) //read operation
    };

    if migrating_block.contains(&current_id) {
        let request = GeneralRequest::LeaseRequest(key, entry);
        if queries.contains_key(&current_id) {
            let requests = queries.get_mut(&current_id).unwrap();
            requests.push(request);
        } else {
            let requests = vec![request];
            queries.insert(current_id, requests);
        }
    } else {
        
        let current_block = {
            let bp_tree = bp_tree.read().unwrap();
            bp_tree.get_block(current_id)
        }; //returns either the leaf or the internal block of the local b-plus tree
        if current_block.is_leaf() {
            if key >= current_block.return_divider_key() {
                //if the key does not belong in this leaf block
                let next_block_id = current_block.return_next_block();
                let request = GeneralRequest::LeaseRequest(key, entry);
                let peers = client.get_providers(next_block_id.to_string()).await;
                let _requests = peers.into_iter().map(|p| {
                    let mut network_client = client.clone();
                    let request = request.clone();
                    async move { network_client.request(p, request).await }.boxed()
                    //flush the query in the next block
                });
            } else {
                let mut bp_tree = bp_tree.write().unwrap();
                let result = bp_tree.insert(current_id, key, entry); //if the block is a leaf then add the entry (write operation)
                match result {
                    InsertResult::Complete => {
                        //if the insertion is successful
                        println!("Success!");
                    }
                    //if it led to a split
                    InsertResult::RightBlock(block_id,divider_key )=> {
                        let id = block_id;
                        let block = bp_tree.get_block(id).clone();
                        let parent = block.parent();
                        client.start_providing(block_id.to_string()).await; //provide until migration is complete


                        //need to inform parent about the existence

                        //if the parent is in the local block map
                        if bp_tree.contains(parent){
                            bp_tree.insert_child(divider_key,block_id,parent);
                        }
                        //else
                        else{
                        let divider_key_request = GeneralRequest::InsertOnRemoteParent(divider_key, block.parent(), id);
                        let peers = client.get_providers(block.parent().to_string()).await;
                        let divider_key_requests = peers.into_iter().map(|p| {
                            let mut network_client = client.clone();
                            let divider_key_request = divider_key_request.clone();
                            async move { network_client.request(p, divider_key_request).await }.boxed()
                        });

                        let info = futures::future::select_ok(divider_key_requests).await;
                        match info{
                            Ok(str) => {
                                // let response:GeneralResponse= serde_json::from_str(&str.0).unwrap();
                                // if let GeneralResponse::InsertOnRemoteParent(parent)=response{
                                //     block.set_parent(parent);

                                // }
                            },
                            Err(err) => println!("Error {:?}", err),
                        };
                        }

                        let migrate_request = GeneralRequest::MigrateRequest(block);
                        migrating_block.insert(id);

                        let result = client.request(*migrate_peer, migrate_request).await;

                        //request migration
                        match result {
                            Ok(_) => {
                                //client.stop_providing(id); stop providing the migrated block
                                bp_tree.remove_block(id); //remove block from local b-plus tree
                                migrating_block.remove(&id); //remove id from record set

                                if queries.contains_key(&id) {
                                    //if there are some queries that are needed to be flushed
                                    let pending_queries = queries.remove(&id).unwrap();
                                    for query in pending_queries {
                                        let result = client.request(*migrate_peer, query).await;
                                        match result {
                                            Ok(str) => {}
                                            Err(err) => {
                                                println!("Error {:?}", err);
                                            }
                                        }
                                    }
                                }
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
        println!("{:?}",providers);
        if providers.is_empty() {
            println!("Could not find provider for lease.");
        }
        let lease = GeneralRequest::LeaseRequest(key, entry); //send a lease request to the next peer
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
}

pub async fn handle_insert_on_remote_parent(
    key: Key,
    parent: BlockId,
    child: BlockId,
    bp_tree: &RwLock<BPTree>,
    client: &mut Client,
    migrate_peer: PeerId,
    migrating_block: &mut HashSet<BlockId>,
    queries: &mut HashMap<BlockId, Vec<GeneralRequest>>,
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
                    GeneralResponse::InsertOnRemoteParent => {
                    }
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
            InsertResult::Complete => {
            }
            //Insertion led to split
            InsertResult::RightBlock(right_block_id,divider_key) => {
                //right block to split
                let block = bp_tree.get_block(right_block_id).clone();
                let migrate_request = GeneralRequest::MigrateRequest(block);
                migrating_block.insert(right_block_id);
                let result = client.request(migrate_peer, migrate_request).await; //request migration
                match result {
                    Ok(_) => {
                        bp_tree.remove_block(right_block_id); //remove block from local b-plus tree
                        migrating_block.remove(&right_block_id); //remove id from record set
                        if queries.contains_key(&right_block_id) {
                            let pending_queries = queries.remove(&right_block_id).unwrap();
                            for query in pending_queries {
                                let result = client.request(migrate_peer, query).await;
                                match result{
                                    Ok(str) => {}
                                    Err(err) => {
                                        println!("Error {:?}", err);
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        println!("Error {:?}", err);
                    }
                }
            }
        }
    }
}

pub async fn handle_migrate(
    block: Block,
    bp_tree: &RwLock<BPTree>,
    client: &mut Client,
) {
    let child_id = block.return_id();
    let mut bp_tree = bp_tree.write().unwrap();
    bp_tree.add_block(child_id, block);
    client.start_providing(child_id.to_string()).await;

}
