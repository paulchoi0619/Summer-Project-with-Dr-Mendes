use super::*;
use libp2p::core::{PeerId};
use libp2p::request_response::{
    ResponseChannel,
};
use std::collections::{HashSet,HashMap};
use network::{GenericResponse,Client};
use bplus::{BPTree,Block,Entry,Key,InsertResult, BlockId};


pub async fn handle_lease_request(key:Key,entry:Entry,bp_tree:&mut BPTree,channel: ResponseChannel<GenericResponse>,receiver:PeerId,
    client: &mut Client){
        let top_id = bp_tree.get_top_id(); //the topmost block id of the local b-plus tree
        let current_id = bp_tree.find(top_id,key); 
        let current_block = bp_tree.get_block(current_id);//returns either the leaf or the internal block of the local b-plus tree
        if current_block.is_leaf(){ 
            let result = bp_tree.insert(current_id,key,entry); //if the block is a leaf then add the entry
            match result{
                InsertResult::Complete =>{ //if the insertion is successful
                    client.respond(GeneralResponse::LeaseResponse(receiver),channel).await; //respond that the insertion is complete
                    //println!("{:?}",bp_tree.get_block_map()); //to check if the entry is added to the block
                }
                InsertResult::InsertOnRemoteParent(parent,key,child) => { //if the parent of the local block is in a remote node
                    let providers = client.get_providers(parent.to_string()).await; //find the parent
                    if providers.is_empty() {
                        println!("Could not find parent.");
                    }
                    let parent_call = GeneralRequest::InsertOnRemoteParent(key,child); //add the child to the remote parent block
                    let requests = providers.into_iter().map(|p| {
                
                        let mut network_client = client.clone();
                        let parent_call = parent_call.clone();
                        async move { network_client.request(p,parent_call).await }.boxed()
                    });
                    let insert_info = futures::future::select_ok(requests).await;
                    match insert_info{
                        Ok(str) => {
                            println!("Here {:?}", str.0);
                            
                        },
                        Err(err) => {
                        println!("Error {:?}", err);
                        }
                    };
                }
            }
            
        }
        else{
            let providers = client.get_providers(current_id.to_string()).await;

            if providers.is_empty() {
                println!("Could not find provider for lease.");
            }
            let lease = GeneralRequest::LeaseRequest(key,entry);
            let requests = providers.into_iter().map(|p| {
                
                let mut network_client = client.clone();
                let lease = lease.clone();
                async move { network_client.request(p,lease).await }.boxed()
            });
            let lease_info = futures::future::select_ok(requests).await;
            match lease_info{
                Ok(str) => {
                    println!("Here {:?}", str.0);
                    
                },
                Err(err) => {
                println!("Error {:?}", err);
                },
            };
    }
}
pub async fn handle_insert_on_remote_parent(key:Key,child:BlockId,bp_tree:&mut BPTree,channel: ResponseChannel<GenericResponse>,
    receiver:PeerId,client:&mut Client){
    let result  = bp_tree.insert_child(key,child);
    match result{
        InsertResult::Complete =>{
            client.respond(GeneralResponse::InsertOnRemoteParent(receiver),channel).await;
        }
        InsertResult::InsertOnRemoteParent(parent,key,child) => {
            let providers = client.get_providers(parent.to_string()).await;
            if providers.is_empty() {
                println!("Could not find parent.");
            }
            let parent_call = GeneralRequest::InsertOnRemoteParent(key,child);
            let requests = providers.into_iter().map(|p| {
        
                let mut network_client = client.clone();
                let parent_call = parent_call.clone();
                async move { network_client.request(p,parent_call).await }.boxed()
            });
            let insert_info = futures::future::select_ok(requests).await;
            match insert_info{
                Ok(str) => {
                    println!("Here {:?}", str.0);
                    
                },
                Err(err) => {
                println!("Error {:?}", err);
                },
            };
        }
                            }
}
pub async fn handle_migrate(mut block:Block,bp_tree:&mut BPTree,channel: ResponseChannel<GenericResponse>,client:&mut Client,new_provider:PeerId){
    let id = block.return_id();
    bp_tree.add_block(id, block);
    //client.start_providing(id.to_string()).await;
    client.start_providing("root".to_string()).await; //for testing purpose
    client.respond(GeneralResponse::MigrateResponse(new_provider),channel).await;
}