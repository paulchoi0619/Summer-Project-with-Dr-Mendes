use super::*;
use bplus::{BPTree, Block, BlockId, Entry, InsertResult, Key};
use libp2p::core::PeerId;
use libp2p::request_response::ResponseChannel;
use network::{Client, GenericResponse};
use std::collections::{HashMap, HashSet};

pub async fn handle_lease_request(
    key: Key,
    entry: Entry,
    bp_tree: &mut BPTree,
    channel: ResponseChannel<GenericResponse>,
    receiver: PeerId,
    client: &mut Client,
    migrate_peer: &PeerId,
    migrating_block: &mut HashSet<BlockId>,
    queries: &mut HashMap<BlockId, Vec<GeneralRequest>>,
) {
    let top_id = bp_tree.get_top_id(); //the topmost block id of the local b-plus tree
    let current_id = bp_tree.find(top_id, key); //read operation

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
        let current_block = bp_tree.get_block(current_id); //returns either the leaf or the internal block of the local b-plus tree

        if current_block.is_leaf() {
            if key >= current_block.return_divider_key() {
                //if the key does not belong in this leaf block
                let request = GeneralRequest::LeaseRequest(key, entry);
                let next_block_id = current_block.return_next_block();
                let peers = client.get_providers(next_block_id.to_string()).await;
                let _requests = peers.into_iter().map(|p| {
                    let mut network_client = client.clone();
                    let request = request.clone();
                    async move { network_client.request(p, request).await }.boxed()
                    //flush the query in the next block
                });
            } else {
                let result = bp_tree.insert(current_id, key, entry); //if the block is a leaf then add the entry (write operation)
                match result {
                    InsertResult::Complete => {
                        //if the insertion is successful
                        client
                            .respond(GeneralResponse::LeaseResponse(receiver), channel)
                            .await; //respond that the insertion is complete
                                    
                    }
                    //if it led to a split
                    InsertResult::RightBlock(block_id) => {
                        client
                            .respond(GeneralResponse::LeaseResponse(receiver), channel)
                            .await; //respond that the insertion is complete
                        
                        //Start migration
                        let id = block_id;
                        let block = bp_tree.get_block(id).clone();
                        client.start_providing(block.parent().to_string()).await;
                        let migrate_request = GeneralRequest::MigrateRequest(block);
                        migrating_block.insert(id);
                        
                        let result = client.request(*migrate_peer, migrate_request).await; 
                        
                        //request migration
                        match result {
                            Ok(_) => {
                                bp_tree.remove_block(id); //remove block from local b-plus tree
                                migrating_block.remove(&id); //remove id from record set
                                if queries.contains_key(&id){ //if there are some queries that are needed to be flushed
                                let pending_queries = queries.remove(&id).unwrap();
                                for query in pending_queries {
                                    let result = client.request(*migrate_peer, query).await;
                                    match result {
                                        Ok(str) => {
                                            println!("{:?}", str);
                                        }
                                        Err(err) => {
                                            println!("Error {:?}", err);
                                        }
                                    }
                                }}
                            }
                            Err(err) => {
                                println!("Error {:?}", err);
                            }
                        }
                    }
                }
                
            }
        } else {
            //this peer does not contain the block
            let providers = client.get_providers(current_id.to_string()).await;

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
                    let response:GeneralResponse= serde_json::from_str(&str.0).unwrap();
                    if let GeneralResponse::LeaseResponse(source)=response{
                    client
                        .respond(GeneralResponse::LeaseResponse(source), channel)
                        .await; //respond that the insertion is complete
                    }
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
    bp_tree: &mut BPTree,
    channel: ResponseChannel<GenericResponse>,
    client: &mut Client,
    migrate_peer: PeerId,
    migrating_block: &mut HashSet<BlockId>,
    queries: &mut HashMap<BlockId, Vec<GeneralRequest>>,
) {
    let parent_block = bp_tree.get_block(parent);
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
                    GeneralResponse::InsertOnRemoteParent(parent) => {
                        client
                            .respond(GeneralResponse::InsertOnRemoteParent(parent), channel)
                            .await;
                    }
                    GeneralResponse::MigrateResponse(_) => {}
                    GeneralResponse::LeaseResponse(_) => {}
                }
            }
            Err(err) => {
                println!("Error {:?}", err);
            }
        };
    } 
    
    else {
        let result = bp_tree.insert_child(key, child, parent);
        match result {
            //Successful insertion
            InsertResult::Complete => {
                client
                    .respond(GeneralResponse::InsertOnRemoteParent(parent), channel)
                    .await;
            }
            //Insertion led to split
            InsertResult::RightBlock(right_block_id) => { //right block to split 
                let block = bp_tree.get_block(right_block_id).clone();
                let migrate_request = GeneralRequest::MigrateRequest(block);
                migrating_block.insert(right_block_id);
                let result = client.request(migrate_peer, migrate_request).await; //request migration
                match result {
                    Ok(_) => {
                        bp_tree.remove_block(right_block_id); //remove block from local b-plus tree
                        migrating_block.remove(&right_block_id); //remove id from record set
                        let left_block_range = bp_tree.get_block(parent).return_divider_key(); //getting the divider key of left block from the split
                        if child < left_block_range{
                            client
                                .respond(GeneralResponse::InsertOnRemoteParent(parent), channel)
                                .await;
                        }   
                        else{
                            client
                                .respond(GeneralResponse::InsertOnRemoteParent(right_block_id), channel)
                                .await;
                        }
                        if queries.contains_key(&right_block_id){
                        let pending_queries = queries.remove(&right_block_id).unwrap();
                        for query in pending_queries {
                            let result = client.request(migrate_peer, query).await;
                            match result {
                                Ok(str) => {
                                    println!("{:?}", str);
                                    
                                }
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
    bp_tree: &mut BPTree,
    channel: ResponseChannel<GenericResponse>,
    client: &mut Client,
    new_provider: PeerId,
) {
    
    let child_id = block.return_id();
    let parent_id = block.parent(); //potential parent of the block
    let key = block.return_parent_key(); //divider key for parent block
    bp_tree.add_block(child_id, block);
    client.start_providing(child_id.to_string()).await;
    client
        .respond(GeneralResponse::MigrateResponse(new_provider), channel)
        .await;
    
    let parent = client.get_providers(parent_id.to_string()).await;
    println!("{:?}",parent_id);
    let parent_call = GeneralRequest::InsertOnRemoteParent(key, parent_id, child_id);
    let requests = parent.into_iter().map(|p| {
        let mut network_client = client.clone();
        let parent_call = parent_call.clone();
        async move { network_client.request(p, parent_call).await }.boxed()
    });
    let insert_info = futures::future::select_ok(requests).await;
    match insert_info {
        Ok(str) => {
            let response: GeneralResponse = serde_json::from_str(&str.0).unwrap();
            match response {
                GeneralResponse::InsertOnRemoteParent(parent) => {
                    let block = bp_tree.get_block(child_id);
                    block.set_parent(parent);
                }
                GeneralResponse::MigrateResponse(_) => {}
                GeneralResponse::LeaseResponse(_) => {}
            }
        }
        Err(err) => {
            println!("Error {:?}", err);
        }
    };
}
