use clap::Parser;
use futures::prelude::*;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::gossipsub::{Topic, IdentTopic, TopicHash};
use libp2p::multiaddr::Protocol;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use tokio;
use tokio::io::AsyncBufReadExt;
use tokio::spawn;
mod events;
use events::{handle_insert_on_remote_parent, handle_lease_request, handle_migrate};
mod bplus;
mod network;
use bplus::{BPTree, Block, BlockId, Entry, Key};
mod gossip_channel;

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
        network::new(secret_key_seed).await?;
    let (mut gossip_command, mut gossip_channel_loop)=
        gossip_channel::new().await?;

    // Spawn the network task for it to run in the background.
    spawn(network_event_loop.run());
    
    //spawn gossip channel
    spawn(gossip_channel_loop.run());

    // In case a listen address was provided use it, otherwise listen on any
    // address.
    // match opt.listen_address {
    match listen_address {
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

    

    let mut block = Block::new(); //initialize block
    block.set_block_id(); 
    let mut bp_tree = BPTree::new(block); //bp tree without the root 
    let mut is_root = false; 

    //let mut topics:Vec<TopicHash> = Vec::new();
    let topic = Topic::new("size"); 
    network_client.subscribe(topic.clone()).await; //subscribe to gossipsub topic

    let mut migrate_peer = network_client_id;
    let mut cur_peer_size = f32::INFINITY as usize;
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();


    let mut migrating_block:HashSet<BlockId> = HashSet::new(); //keeping track of blocks that are in the progress of migration
    let mut queries:HashMap<BlockId,Vec<GeneralRequest>> = HashMap::new(); //storing commands to send after migration is complete
    
    loop {
        tokio::select! {
            line_option = stdin.next_line() =>
            match line_option {
                Ok(None) => {break;},
                Ok(Some(line)) => {
                    match line.as_str() {
                        cmd if cmd.starts_with("getlease") => {
                            println!("Type key:");

                            let key= stdin.next_line().await;
                            match key {
                                        Ok(None) => {
                                        println!("Missing Key");
                                        break;
                                    },
                                        Ok(Some(line)) => {
                                        let input = line.parse::<u64>();
                                        match input{
                                            Ok(key) =>{
                                                
                                                
                                            if is_root{ //if this peer is the node
                                                // handle_root_request(key,entry,&mut bp_tree,&mut network_client).await;
                                                }
                                            else{
                                               
                                                let providers = network_client.get_providers("root".to_string()).await;
                                                if providers.is_empty() {
                                                    return Err(format!("Could not find provider for leases.").into());
                                                }
                                                let requests = providers.into_iter().map(|p| {
                                                    let entry = Entry::new(network_client_id,key);
                                                    let lease = GeneralRequest::LeaseRequest(key,entry);
                                                    let mut network_client = network_client.clone();
                                                    //let lease = lease.clone();
                                                    async move { network_client.request(p,lease).await }.boxed()
                                                });

                                            let lease_info = futures::future::select_ok(requests)
                                                .await;

                                            match lease_info{
                                                Ok(str) => {
                                                    let response:GeneralResponse= serde_json::from_str(&str.0).unwrap();
                                                    println!("Here {:?}", response);
                                                },
                                                Err(err) => println!("Error {:?}", err),
                                            };
                                            }
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

                        },
                        cmd if cmd.starts_with("root") => {
                            let providers = network_client.get_providers("root".to_string()).await;
                            if providers.is_empty() {
                                network_client.boot_root().await;
                                is_root = true;
                            }
                            else{
                                println!("root already exists!")
                            }
                        },
                        cmd if cmd.starts_with("migrate") => {
                                let id = bp_tree.get_top_id();
                                let block = bp_tree.get_block(id).clone();
                                let migrate_request = GeneralRequest::MigrateRequest(block);
                                let result = network_client.request(migrate_peer,migrate_request).await;
                                match result{
                                    Ok(str) => {
                                        let response:GeneralResponse= serde_json::from_str(&str).unwrap();
                                        println!("New Provider {:?}", response);
                                    },
                                    Err(err) => println!("Error {:?}", err),
                                };

                        }

                        _ => println!("unknown command\n"),
                    }
                },
                Err(_) => print!("Error handing input line: "),
            },
            gossip = gossip_command.next() => match gossip{ //initiates gossip
                None => {
                },
                Some(_) => {
                    let size = bp_tree.get_size();
                    network_client.publish(topic.clone(), size).await;
                }
            },
            event = network_events.next() => match event {
                    None => {
                    },
                    Some(network::Event::InboundRequest {request, channel }) => {
                        
                        let response:GeneralRequest= serde_json::from_str(&request).unwrap();
                        match response{

                            GeneralRequest::LeaseRequest(key,entry) => { //request response channel
                                handle_lease_request(key, entry, &mut bp_tree, channel,network_client_id,&mut network_client,migrate_peer,
                                &mut migrating_block,&mut queries).await;
                                println!("{:?}",bp_tree.get_block_map());
                            }
                            GeneralRequest::MigrateRequest(block)=>{
                                handle_migrate(block,&mut bp_tree,channel,&mut network_client,network_client_id).await;
                                println!("{:?}",bp_tree.get_block_map());

                            }
                            GeneralRequest::InsertOnRemoteParent(divider_key,parent_id,child_id) =>{
                                handle_insert_on_remote_parent(divider_key, parent_id,child_id, &mut bp_tree, channel,
                                &mut network_client,migrate_peer,&mut migrating_block,&mut queries).await;
                                println!("{:?}",bp_tree.get_block_map());
                            }
                        }

                    },
                    Some(network::Event::InboundGossip{message}) => {
                        let source_id = message.source.unwrap();
                        let data = String::from_utf8_lossy(&message.data);
                        let peer_size:usize = serde_json::from_str(&data).unwrap();
                        if peer_size<cur_peer_size{
                            cur_peer_size = peer_size;
                            migrate_peer = source_id;

                        }
                    },
                    Some(network::Event::Subscribed{topic}) => {
                    //in case needed in the future
                    }
                },
        }
    }

    Ok(())
}


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

#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub enum GeneralRequest {
    LeaseRequest(Key, Entry),
    MigrateRequest(Block),
    InsertOnRemoteParent(Key, BlockId,BlockId),
}
#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub enum GeneralResponse {
    LeaseResponse(PeerId),
    MigrateResponse(PeerId),
    InsertOnRemoteParent(BlockId),
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
