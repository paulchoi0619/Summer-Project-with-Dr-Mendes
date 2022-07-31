use futures::future::Either::{Left,Right};
use tokio;
use clap::Parser;
use tokio::spawn;
use tokio::io::AsyncBufReadExt;
use futures::prelude::*;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::multiaddr::Protocol;
use std::error::Error;
use std::path::PathBuf;
use serde_json;

use std::hash::{Hash, Hasher};
use serde::{Serialize, Deserialize};

mod network;
mod bplus;
use bplus::{BPTree,Block,SplitResult,Entry,Data,SIZE,Key,InsertResult, BlockId};

// run with cargo run -- --secret-key-seed #

#[tokio::main] 
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let opt = Opt::parse();
    // manual args
    // let secret_key_seed: Option<u8> = Some(1);
    let secret_key_seed = opt.secret_key_seed;
    let listen_address: Option<Multiaddr> = None;
    let peer: Option<Multiaddr> = None;

    let (mut network_client, mut network_events, network_event_loop, network_client_id) =
    // network::new(opt.secret_key_seed).await?;
    network::new(secret_key_seed).await?;

    println!("my id: {:?}", network_client_id);

    // Spawn the network task for it to run in the background.
    spawn(network_event_loop.run());

    // In case a listen address was provided use it, otherwise listen on any
    // address.
    // match opt.listen_address {
    match listen_address{
        Some(addr) => network_client
            .start_listening(addr)
            .await
            .expect("Listening not to fail."),
        None => network_client
            .start_listening("/ip4/0.0.0.0/tcp/0".parse()?)
            .await
            .expect("Listening not to fail."),
    };
    
    // In case the user provided an address of a peer on the CLI, dial it.
    // if let Some(addr) = opt.peer {
    if let Some(addr) = peer {
        let peer_id = match addr.iter().last() {
            Some(Protocol::P2p(hash)) => PeerId::from_multihash(hash).expect("Valid hash."),
            _ => return Err("Expect peer multiaddr to contain peer ID.".into()),
        };
        network_client
            .dial(peer_id, addr)
            .await
            .expect("Dial to succeed");
    }

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    

    let mut block = Block::new();
    block.set_block_id();
    let mut bp_tree = BPTree::new(block);
    // to be found in the network with the specific name
    loop {
        tokio::select! { 
            line_option = stdin.next_line() => 
            match line_option {
                Ok(None) => {break;},
                Ok(Some(line)) => {
                    match line.as_str() {
                        // "getlease" => list_peers().await,
                        cmd if cmd.starts_with("getlease") => {
                            println!("Type key:");
                            tokio::select! { 
                            key= stdin.next_line() =>
                            match key {
                                        Ok(None) => {
                                        println!("Missing Key");
                                        break;},
                                        Ok(Some(line)) => {   
                                        let input = line.parse::<u64>();
                                        match input{
                                            Ok(key) =>{
                                                let entry = Entry::new(network_client_id,key);
                                                let lease = GeneralRequest::LeaseRequest(key,entry);
                                               
                                                let providers = network_client.get_providers("root".to_string()).await;

                                                if providers.is_empty() {
                                                    return Err(format!("Could not find provider for leases.").into());
                                                }

                                                let requests = providers.into_iter().map(|p| {
                                                    
                                                    let mut network_client = network_client.clone();
                                                    let lease = lease.clone();
                                                    async move { network_client.request(p,lease).await }.boxed()
                                                });
                                            
                                            let lease_info = futures::future::select_ok(requests)
                                                .await;
                                        
                                            match lease_info{
                                                Ok(str) => println!("Here {:?}", str.0),
                                                Err(err) => println!("Error {:?}", err),
                                            };
                                            }
                                            Err(_) =>{
                                                println!("Incorrect Key")
                                            }
                                        }
                                             
                                        }
                            Err(_) =>{
                                println!("Error")
                            }
                        }
                    }
                            
                        },
                        cmd if cmd.starts_with("root") => {
                            let providers = network_client.get_providers("root".to_string()).await;
                            if providers.is_empty() {
                                network_client.boot_root().await;
                            } 
                            else{
                                println!("root already exists!")
                            }
                        },

                        _ => println!("unknown command\n"),
                    }
                },
                Err(_) => print!("Error handing input line: "),
            },
            event = network_events.next() => match event {
                    None => {
                    },
                    Some(network::Event::InboundRequest {request, channel }) => {
                        let response:GeneralRequest= serde_json::from_str(&request).unwrap();
                        match response{
                            GeneralRequest::LeaseRequest(key,entry) => {
                                network_client.handle_lease_request(key, entry, &mut bp_tree, channel).await;
                            }
                            GeneralRequest::MigrateRequest(Block)=>{
                            
                            }
                            GeneralRequest::InsertOnRemoteParent(Key,BlockId) =>{
                                network_client.handle_insert_on_remote_parent(Key, BlockId, &mut bp_tree, channel).await;
                            }
                        }                       
                        
                    }
                },
        }
    }

    Ok(())
}








// internal node extra field: children
//impl block 
// search : child if internal node or else entry
//fn search(searched: Entry) -> Either<Entry, BlockId>




#[derive(Parser, Debug)]
#[clap(name = "libp2p file sharing example")]
struct Opt {
    /// Fixed value to generate deterministic peer ID.
    #[clap(long)]
    secret_key_seed: Option<u8>,

    // #[clap(long)]
    // peer: Option<Multiaddr>,

    // #[clap(long)]
    // listen_address: Option<Multiaddr>,

    // #[clap(subcommand)]
    // argument: CliArgument,
}



#[derive(Debug,Serialize,Deserialize,Clone,Hash)]
pub enum GeneralRequest{
    LeaseRequest (Key,Entry),
    MigrateRequest(Block),
    InsertOnRemoteParent(Key,BlockId),
}




#[derive(Debug, Parser)]
enum CliArgument {
    Provide {
        #[clap(long)]
        path: PathBuf,
        #[clap(long)]
        name: String,
    },
    Get {
        #[clap(long)]
        name: String,
    },
    BeRoot {
        #[clap(long)]
        name: String,
    },
    GetLease {
        #[clap(long)]
        name: String,
        #[clap(long)]
        data: String,
    },
}


