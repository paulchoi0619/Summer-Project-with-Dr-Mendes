use clap::Parser;
use futures::prelude::*;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::gossipsub::Topic;
use libp2p::multiaddr::Protocol;
use libp2p::request_response::ResponseChannel;
use network::{Client, GenericResponse};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::thread;
use tokio;
use tokio::io::AsyncBufReadExt;
use tokio::spawn;
mod events;
use events::{handle_insert_on_remote_parent, handle_lease_request, handle_migrate};
mod bplus;
mod network;
use bplus::{BPTree, Block, BlockId, Entry, Key};
mod gossip_timer;

// run with cargo run -- --secret-key-seed #

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let opt = Opt::parse();
    // manual args
    //let secret_key_seed: Option<u8> = Some(1);
    let secret_key_seed = opt.secret_key_seed;
    let listen_address: Option<Multiaddr> = None;
    let peer: Option<Multiaddr> = None;

    let (mut network_client, mut network_events, network_event_loop, network_client_id) =
        network::new(secret_key_seed).await?;
    let (mut gossip_command, gossip_timer_loop) = gossip_timer::new().await?;

    // Spawn the network task for it to run in the background.
    spawn(network_event_loop.run());

    //spawn gossip timer
    spawn(gossip_timer_loop.run());

    // In case a listen address was provided use it, otherwise listen on any
    // address.
    // match opt.listen_address {
    match listen_address {
        Some(addr) => network_client
            .start_listening(addr, network_client_id)
            .await
            .expect("Listening not to fail."),
        None => network_client
            .start_listening("/ip4/0.0.0.0/tcp/0".parse()?, network_client_id)
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

    let bp_tree = Arc::new(RwLock::new(BPTree::new())); //initialize bp_tree
    let mut is_root = false;

    let topic = Topic::new("size");

    let mut migrate_peer = network_client_id;
    let mut cur_peer_size = f32::INFINITY as usize;
    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    let migrating_block: Arc<RwLock<HashSet<BlockId>>> = Arc::new(RwLock::new(HashSet::new())); //keeping track of blocks that are in the progress of migration
    let queries: Arc<RwLock<HashMap<BlockId, Vec<GeneralRequest>>>> =
        Arc::new(RwLock::new(HashMap::new())); //storing commands to send after migration is complete

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
                                                let entry = Entry::new(network_client_id,key);
                                                if is_root{ //it is the provider of the root
                                                    let read_top_id = bp_tree.read().unwrap();
                                                    let top_id = read_top_id.get_top_id();
                                                    let bp_tree = bp_tree.clone();
                                                    let migrating_block = migrating_block.clone();
                                                    let queries = queries.clone();
                                                    let mut clone_client = network_client.clone();
                                                    thread::spawn(move ||{
                                                        let rt = tokio::runtime::Runtime::new().unwrap();
                                                        rt.block_on( handle_lease_request(key,entry,bp_tree,&mut clone_client,migrate_peer,migrating_block,queries,top_id));
                                                    });
                                                }   
                                                else{
                                                let providers = network_client.get_providers("root".to_string()).await;
                                                if providers.is_empty() {
                                                    return Err(format!("Could not find provider for leases.").into());
                                                }



                                            let mut copy_network_client = network_client.clone();
                                            let mut p = network_client_id;
                                            for i in providers.iter(){
                                                p = *i;
                                            }
                                            let id = Default::default();
                                            let lease = GeneralRequest::LeaseRequest(key,entry,id);
                                            let result = copy_network_client.request(p,lease).await;

                                            match result{
                                                Ok(result)=>{
                                                    println!("Done");
                                                },
                                                Err(_)=>{
                                                    println!("Error");
                                                }
                                            }
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
                                let mut block = Block::new(); //initialize block
                                block.set_block_id(); //initialize block id
                                let top_id = block.return_id();
                                let bp_tree = bp_tree.clone();
                                let mut bp_tree = bp_tree.write().unwrap();
                                bp_tree.add_block(top_id,block); //insert block in map
                                bp_tree.set_top_id(top_id); //set the top id
                                network_client.boot_root().await;
                                network_client.subscribe(topic.clone()).await; //subscribe to gossipsub topic
                                is_root = true;
                            }
                            else{
                                println!("root already exists!")
                            }
                        },
                        cmd if cmd.starts_with("migrate") => {
                                let bp_tree = bp_tree.read().unwrap();
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
                   
                    let tree_size = bp_tree.read().unwrap();
                    let size = tree_size.get_size();
                    network_client.publish(topic.clone(), size).await;
                }
            },
            event = network_events.next() => match event {
                    None => {
                    },
                    Some(network::Event::InboundRequest {request, channel }) => {

                        let response:GeneralRequest= serde_json::from_str(&request).unwrap();
                        let copy_bp_tree = bp_tree.clone();
                        let mut clone_client = network_client.clone();
                        let migrating_block = migrating_block.clone();
                        let queries = queries.clone();
                        match response{
                            GeneralRequest::LeaseRequest(key,entry,block_id) => { //request response channel
                                let mut current_id = block_id;
                                if (is_root){
                                    let read_id = bp_tree.read().unwrap();
                                    current_id = read_id.get_top_id();
                                }
                                thread::spawn(move ||{
                                    let rt = tokio::runtime::Runtime::new().unwrap();
                                    rt.block_on(clone_client.respond(GeneralResponse::LeaseResponse, channel));
                                    rt.block_on(handle_lease_request(key, entry, copy_bp_tree,&mut clone_client,migrate_peer,
                                        migrating_block,queries,current_id));
                                });

                            }
                            GeneralRequest::MigrateRequest(block)=>{
                                thread::spawn(move||{
                                let rt = tokio::runtime::Runtime::new().unwrap();
                                rt.block_on(clone_client.respond(GeneralResponse::MigrateResponse, channel));
                                rt.block_on(handle_migrate(block,copy_bp_tree,&mut clone_client));
                                });
                            }
                            GeneralRequest::InsertOnRemoteParent(divider_key,parent_id,child_id) =>{
                                thread::spawn(move||{
                                let rt = tokio::runtime::Runtime::new().unwrap();
                                rt.block_on(clone_client.respond(GeneralResponse::InsertOnRemoteParent, channel));
                                rt.block_on(handle_insert_on_remote_parent(divider_key, parent_id,child_id, copy_bp_tree,
                                &mut clone_client,migrate_peer,migrating_block,queries));
                                });
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
    LeaseRequest(Key, Entry,BlockId),
    MigrateRequest(Block),
    InsertOnRemoteParent(Key, BlockId, BlockId),
}
#[derive(Debug, Serialize, Deserialize, Clone, Hash)]
pub enum GeneralResponse {
    LeaseResponse,
    MigrateResponse,
    InsertOnRemoteParent,
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
