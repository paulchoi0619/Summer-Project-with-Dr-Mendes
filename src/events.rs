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
    migrate_peer: PeerId,
    migrating_block: &mut HashSet<BlockId>,
    queries: &mut HashMap<BlockId, Vec<GeneralRequest>>,
) {
    let top_id = bp_tree.get_top_id(); //the topmost block id of the local b-plus tree
    let current_id = bp_tree.find(top_id, key);

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
                let next_block_id = bp_tree.retrieve_next(current_id);
                let peers = client.get_providers(next_block_id.to_string()).await;
                let _requests = peers.into_iter().map(|p| {
                    let mut network_client = client.clone();
                    let request = request.clone();
                    async move { network_client.request(p, request).await }.boxed()
                    //flush the query in the next block
                });
            } else {
                let result = bp_tree.insert(current_id, key, entry); //if the block is a leaf then add the entry
                match result {
                    InsertResult::Complete => {
                        //if the insertion is successful
                        client
                            .respond(GeneralResponse::LeaseResponse(receiver), channel)
                            .await; //respond that the insertion is complete
                                    //println!("{:?}",bp_tree.get_block_map()); //to check if the entry is added to the block
                    }
                    InsertResult::RightBlock(block_id) => {
                        let id = block_id;
                        let block = bp_tree.get_block(id).clone();
                        let migrate_request = GeneralRequest::MigrateRequest(block);
                        migrating_block.insert(id);
                        let result = client.request(migrate_peer, migrate_request).await; //request migration
                        match result {
                            Ok(_) => {
                                bp_tree.remove_block(id); //remove block from local b-plus tree
                                migrating_block.remove(&id); //remove id from record set
                                let pending_queries = queries.remove(&id).unwrap();
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
                            Err(err) => {
                                println!("Error {:?}", err);
                            }
                        }
                    }
                }
            }
        } else {
            let providers = client.get_providers(current_id.to_string()).await;

            if providers.is_empty() {
                println!("Could not find provider for lease.");
            }
            let lease = GeneralRequest::LeaseRequest(key, entry);
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
        let right_block = bp_tree.retrieve_next(parent); //adjacent block of the internal block
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
    } else {
        let result = bp_tree.insert_child(key, child, parent);
        match result {
            InsertResult::Complete => {
                client
                    .respond(GeneralResponse::InsertOnRemoteParent(parent), channel)
                    .await;
            }
            InsertResult::RightBlock(block_id) => {
                client
                    .respond(GeneralResponse::InsertOnRemoteParent(parent), channel)
                    .await;
                let id = block_id;
                let block = bp_tree.get_block(id).clone();
                let migrate_request = GeneralRequest::MigrateRequest(block);
                migrating_block.insert(id);
                let result = client.request(migrate_peer, migrate_request).await; //request migration
                match result {
                    Ok(_) => {
                        bp_tree.remove_block(id); //remove block from local b-plus tree
                        migrating_block.remove(&id); //remove id from record set
                        let pending_queries = queries.remove(&id).unwrap();
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
