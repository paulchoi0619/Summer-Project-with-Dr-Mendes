use super::*;

use libp2p::core::PeerId;
use libp2p::request_response::ResponseChannel;

use std::collections::{HashMap, HashSet};

use bplus::{BPTree, Block, BlockId, Entry, InsertResult, Key};
use network::{Client, GenericResponse};

pub async fn handle_lease_request(
    key: Key,
    entry: Entry,
    bp_tree: &mut BPTree,
    channel: ResponseChannel<GenericResponse>,
    receiver: PeerId,
    block_migration: &HashSet<BlockId>,
    commands: &mut HashMap<BlockId, Vec<GeneralRequest>>,
    client: &mut Client,
) {
    let top_id = bp_tree.get_top_id();
    let current_id = bp_tree.find(top_id, key);
    let current_block = bp_tree.get_block(current_id);
    if block_migration.contains(&current_id) {
        if commands.contains_key(&current_id) {
            let requests = commands.get_mut(&current_id).unwrap();
            requests.push(GeneralRequest::LeaseRequest(key, entry));
        } else {
            commands.insert(current_id, vec![GeneralRequest::LeaseRequest(key, entry)]);
        }
        return;
    }
    if current_block.is_leaf() {
        let result = bp_tree.insert(current_id, key, entry);
        match result {
            InsertResult::Complete => {
                client
                    .respond(GeneralResponse::LeaseResponse(receiver), channel)
                    .await;
                println!("{:?}", bp_tree.get_block_map());
            }
            InsertResult::InsertOnRemoteParent(parent, key, child) => {
                let providers = client.get_providers(parent.to_string()).await;
                if providers.is_empty() {
                    println!("Could not find parent.");
                }
                let parent_call = GeneralRequest::InsertOnRemoteParent(key, child);
                let requests = providers.into_iter().map(|p| {
                    let mut network_client = client.clone();
                    let parent_call = parent_call.clone();
                    async move { network_client.request(p, parent_call).await }.boxed()
                });
                let insert_info = futures::future::select_ok(requests).await;
                match insert_info {
                    Ok(str) => {
                        println!("Here {:?}", str.0);
                    }
                    Err(err) => {
                        println!("Error {:?}", err);
                    }
                };
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
pub async fn handle_lease_requests(
    key: Key,
    entry: Entry,
    bp_tree: &mut BPTree,
    block_migration: &HashSet<BlockId>,
    commands: &mut HashMap<BlockId, Vec<GeneralRequest>>,
    client: &mut Client,
) {
    let top_id = bp_tree.get_top_id();
    let current_id = bp_tree.find(top_id, key);
    let current_block = bp_tree.get_block(current_id);
    if block_migration.contains(&current_id) {
        if commands.contains_key(&current_id) {
            let requests = commands.get_mut(&current_id).unwrap();
            requests.push(GeneralRequest::LeaseRequest(key, entry));
        } else {
            commands.insert(current_id, vec![GeneralRequest::LeaseRequest(key, entry)]);
        }
        return;
    }
    if current_block.is_leaf() {
        let result = bp_tree.insert(current_id, key, entry);
        match result {
            InsertResult::Complete => {
                return;
            }
            InsertResult::InsertOnRemoteParent(parent, key, child) => {
                let providers = client.get_providers(parent.to_string()).await;
                if providers.is_empty() {
                    println!("Could not find parent.");
                }
                let parent_call = GeneralRequest::InsertOnRemoteParent(key, child);
                let requests = providers.into_iter().map(|p| {
                    let mut network_client = client.clone();
                    let parent_call = parent_call.clone();
                    async move { network_client.request(p, parent_call).await }.boxed()
                });
                let insert_info = futures::future::select_ok(requests).await;
                match insert_info {
                    Ok(str) => {
                        println!("Here {:?}", str.0);
                    }
                    Err(err) => {
                        println!("Error {:?}", err);
                    }
                };
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
pub async fn handle_insert_on_remote_parent(
    key: Key,
    child: BlockId,
    bp_tree: &mut BPTree,
    channel: ResponseChannel<GenericResponse>,
    receiver: PeerId,
    client: &mut Client,
) {
    let result = bp_tree.insert_child(key, child);
    match result {
        InsertResult::Complete => {
            client
                .respond(GeneralResponse::InsertOnRemoteParent(receiver), channel)
                .await;
        }
        InsertResult::InsertOnRemoteParent(parent, key, child) => {
            let providers = client.get_providers(parent.to_string()).await;
            if providers.is_empty() {
                println!("Could not find parent.");
            }
            let parent_call = GeneralRequest::InsertOnRemoteParent(key, child);
            let requests = providers.into_iter().map(|p| {
                let mut network_client = client.clone();
                let parent_call = parent_call.clone();
                async move { network_client.request(p, parent_call).await }.boxed()
            });
            let insert_info = futures::future::select_ok(requests).await;
            match insert_info {
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
pub async fn handle_migrate(
    mut block: Block,
    bp_tree: &mut BPTree,
    channel: ResponseChannel<GenericResponse>,
    client: &mut Client,
) {
    let id = block.return_id();
    bp_tree.add_block(id, block);
    client
        .respond(GeneralResponse::MigrateResponse, channel)
        .await;
    client.start_providing(id.to_string()).await;
}
