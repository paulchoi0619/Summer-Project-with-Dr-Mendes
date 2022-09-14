
use super::*;
use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use libp2p::core::either::EitherError;
use libp2p::core::upgrade::{read_length_prefixed, write_length_prefixed, ProtocolName};
use libp2p::gossipsub::error::{GossipsubHandlerError, SubscriptionError};
use libp2p::gossipsub::{
    GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity, MessageId,
    ValidationMode,
};
use libp2p::identity::ed25519;
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{
    GetClosestPeersOk, GetProvidersOk, Kademlia, KademliaEvent, QueryId, QueryResult,
};
use libp2p::mdns::{Mdns, MdnsConfig, MdnsEvent};
use libp2p::multiaddr::Protocol;
use libp2p::request_response::{
    ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec, RequestResponseEvent,
    RequestResponseMessage, ResponseChannel,
};
use libp2p::swarm::{ConnectionHandlerUpgrErr, SwarmBuilder, SwarmEvent};
use libp2p::{gossipsub, identity};
use libp2p::{NetworkBehaviour, Swarm};
use serde_json;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::iter;
use tokio::io;
use tokio::time::Duration;
use void;

/// Creates the network components, namely:
///
/// - The network client to interact with the network layer from anywhere
///   within your application.
///
/// - The network event stream, e.g. for incoming requests.
///
/// - The network task driving the network itself.
pub async fn new(
    secret_key_seed: Option<u8>,
) -> Result<(Client, impl Stream<Item = Event>, EventLoop, PeerId), Box<dyn Error>> {
    // Create a public/private key pair, either random or based on a seed.
    let id_keys = match secret_key_seed {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            let secret_key = ed25519::SecretKey::from_bytes(&mut bytes).expect(
                "this returns `Err` only if the length is wrong; the length is correct; qed",
            );
            identity::Keypair::Ed25519(secret_key.into())
        }
        None => identity::Keypair::generate_ed25519(),
    };

    let peer_id = id_keys.public().to_peer_id();

    let message_id_fn = |message: &GossipsubMessage| {
        let mut s = DefaultHasher::new();
        message.data.hash(&mut s);
        MessageId::from(s.finish().to_string())
    };

    let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
        .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
        .message_id_fn(message_id_fn)
        .build()
        .expect("Valid config");

    let mut gossipsub: gossipsub::Gossipsub = gossipsub::Gossipsub::new(
        MessageAuthenticity::Signed(id_keys.clone()),
        gossipsub_config,
    )?;

    // Build the Swarm, connecting the lower layer transport logic with the
    // higher layer network behaviour logic.
    let swarm = SwarmBuilder::new(
        libp2p::development_transport(id_keys.clone()).await?,
        ComposedBehaviour {
            kademlia: Kademlia::new(peer_id, MemoryStore::new(peer_id)),
            request_response: RequestResponse::new(
                GenericExchangeCodec(),
                iter::once((GenericProtocol(), ProtocolSupport::Full)),
                Default::default(),
            ),
            mdns: Mdns::new(MdnsConfig::default()).await?,
            gossipsub: gossipsub,
        },
        peer_id,
    )
    .build();

    let (command_sender, command_receiver) = mpsc::channel(0);
    let (event_sender, event_receiver) = mpsc::channel(0);

    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
        peer_id,
    ))
}

#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<Command>,
}

impl Client {
    /// Listen for incoming connections on the given address.

    pub async fn start_listening(&mut self, addr: Multiaddr) -> Result<(), Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartListening { addr, sender })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.")
    }

    /// Dial the given peer at the given address.
    pub async fn dial(
        &mut self,
        peer_id: PeerId,
        peer_addr: Multiaddr,
    ) -> Result<(), Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Dial {
                peer_id,
                peer_addr,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        println!("dialed {:?}", peer_id);
        receiver.await.expect("Sender not to be dropped.")
    }
    /// Advertise the local node as the provider of the given file on the DHT.
    pub async fn start_providing(&mut self, file_name: String) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartProviding { file_name, sender })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.");
    }
    /// Find the providers for the given file on the DHT.
    pub async fn get_providers(&mut self, file_name: String) -> HashSet<PeerId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::GetProviders { file_name, sender })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.")
    }

    pub async fn get_closest_peer(&mut self, id: PeerId) -> Vec<PeerId> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::GetClosestPeers { id, sender })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.")
    }

    pub async fn request(
        &mut self,
        peer: PeerId,
        request: GeneralRequest,
    ) -> Result<String, Box<dyn Error + Send>> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Request {
                peer,
                request,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not be dropped.")
    }

    pub async fn respond(
        &mut self,
        response: GeneralResponse,
        channel: ResponseChannel<GenericResponse>,
    ) {
        self.sender
            .send(Command::Respond { response, channel })
            .await
            .expect("Command receiver not to be dropped.");
    }

    pub async fn boot_root(&mut self) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::BootRoot {
                up_root: "root".to_string(),
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.");
    }
    pub async fn subscribe(&mut self, topic: Topic) {
        self.sender
            .send(Command::Subscribe { topic })
            .await
            .expect("Command receiver not to be dropped.");
    }
    pub async fn publish(&mut self, topic: Topic, size: usize) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Publish {
                topic,
                size,
                sender,
            })
            .await
            .expect("Command receiver not to be dropped.");
        receiver.await.expect("Sender not to be dropped.");
    }
}

pub struct EventLoop {
    swarm: Swarm<ComposedBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), Box<dyn Error + Send>>>>,
    pending_start_providing: HashMap<QueryId, oneshot::Sender<()>>,
    pending_get_providers: HashMap<QueryId, oneshot::Sender<HashSet<PeerId>>>,
    pending_get_closest_peers: HashMap<QueryId, oneshot::Sender<Vec<PeerId>>>,
    pending_request: HashMap<RequestId, oneshot::Sender<Result<String, Box<dyn Error + Send>>>>,
    pending_publish: HashMap<MessageId, oneshot::Sender<(PeerId, GossipsubMessage)>>,
}
impl EventLoop {
    fn new(
        swarm: Swarm<ComposedBehaviour>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
            pending_dial: Default::default(),
            pending_start_providing: Default::default(),
            pending_get_providers: Default::default(),
            pending_request: Default::default(),
            pending_get_closest_peers: Default::default(),
            pending_publish: Default::default(),
        }
    }

    pub async fn run(mut self) {
        loop {
            futures::select! {
                event = self.swarm.next() => self.handle_event(event.expect("Swarm stream to be infinite.")).await  ,
                command = self.command_receiver.next() => match command {
                    Some(c) => self.handle_command(c).await,
                    // Command channel closed, thus shutting down the network event loop.
                    None=>  return,
                },
            }
        }
    }

    async fn handle_event(
        &mut self,
        event: SwarmEvent<
            ComposedEvent,
            EitherError<
                EitherError<
                    EitherError<ConnectionHandlerUpgrErr<io::Error>, io::Error>,
                    void::Void,
                >,
                GossipsubHandlerError,
            >,
        >,
    ) {
        match event {
            SwarmEvent::Behaviour(ComposedEvent::Mdns(MdnsEvent::Discovered(discovered_list))) => {
                for (peer, addr) in discovered_list {
                    self.swarm.behaviour_mut().kademlia.add_address(&peer, addr);
                    println!("added: {:?}", peer);
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(&peer);
                }
            }
            SwarmEvent::Behaviour(ComposedEvent::Mdns(MdnsEvent::Expired(expired_list))) => {
                // for (peer, _addr) in expired_list {
                //     if !self.swarm.behaviour_mut().mdns.has_node(&peer) {
                //         self
                //             .swarm
                //             .behaviour_mut()
                //             .kademlia
                //             .remove_peer(&peer);
                //         self
                //             .swarm
                //             .behaviour_mut()
                //             .gossipsub
                //             .remove_explicit_peer(&peer);
                //     }
                // }
            }
            SwarmEvent::Behaviour(ComposedEvent::Gossipsub(GossipsubEvent::Message {
                propagation_source: peer_id,
                message_id: id,
                message,
            })) => {
                //println!("Got message: {} with id: {} from peer: {:?}", String::from_utf8_lossy(&message.data), id, peer_id);
                let _ = self
                    .pending_publish
                    .remove(&id)
                    .expect("Completed")
                    .send((peer_id, message));
            }
            SwarmEvent::Behaviour(ComposedEvent::Gossipsub(GossipsubEvent::Subscribed {
                peer_id,
                topic,
            })) => {
                println!("Subscribed to: {} from peer: {:?}", topic, peer_id);
            }

            SwarmEvent::Behaviour(ComposedEvent::Kademlia(
                KademliaEvent::OutboundQueryCompleted {
                    id,
                    result: QueryResult::GetClosestPeers(Ok(GetClosestPeersOk { peers, .. })),
                    ..
                },
            )) => {
                let _ = self
                    .pending_get_closest_peers
                    .remove(&id)
                    .expect("Completed query to be previously pending.")
                    .send(peers);
            }
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(
                KademliaEvent::OutboundQueryCompleted {
                    id,
                    result: QueryResult::StartProviding(_),
                    ..
                },
            )) => {
                let sender: oneshot::Sender<()> = self
                    .pending_start_providing
                    .remove(&id)
                    .expect("Completed query to be previously pending.");
                let _ = sender.send(());
            }
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(
                KademliaEvent::OutboundQueryCompleted {
                    id,
                    result: QueryResult::GetProviders(Ok(GetProvidersOk { providers, .. })),
                    ..
                },
            )) => {
                println!("B Received GetProvider Response");
                let _ = self
                    .pending_get_providers
                    .remove(&id)
                    .expect("Completed query to be previously pending.")
                    .send(providers);
            }
            SwarmEvent::Behaviour(ComposedEvent::Kademlia(_)) => {}
            SwarmEvent::Behaviour(ComposedEvent::RequestResponse(
                RequestResponseEvent::Message { message, .. },
            )) => match message {
                RequestResponseMessage::Request {
                    request, channel, ..
                } => {
                    self.event_sender
                        .send(Event::InboundRequest {
                            request: request.0,
                            channel,
                        })
                        .await
                        .expect("Event receiver not to be dropped.");
                }
                RequestResponseMessage::Response {
                    request_id,
                    response,
                } => {
                    let _ = self
                        .pending_request
                        .remove(&request_id)
                        .expect("Request to still be pending.")
                        .send(Ok(response.0));
                    // don't wrap it in an Ok - or do
                    // send the whole response
                }
            },
            SwarmEvent::Behaviour(ComposedEvent::RequestResponse(
                RequestResponseEvent::OutboundFailure {
                    request_id, error, ..
                },
            )) => {
                let _ = self
                    .pending_request
                    .remove(&request_id)
                    .expect("Request to still be pending.")
                    .send(Err(Box::new(error)));
            }
            SwarmEvent::Behaviour(ComposedEvent::RequestResponse(
                RequestResponseEvent::ResponseSent { .. },
            )) => {}
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = *self.swarm.local_peer_id();
                println!(
                    "Local node is listening on {:?}",
                    address.with(Protocol::P2p(local_peer_id.into()))
                );
            }
            SwarmEvent::IncomingConnection { .. } => {}
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                if endpoint.is_dialer() {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Ok(()));
                    }
                }
            }
            SwarmEvent::ConnectionClosed { .. } => {}
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Err(Box::new(error)));
                    }
                }
            }
            SwarmEvent::IncomingConnectionError { .. } => {}
            SwarmEvent::Dialing(peer_id) => println!("Dialing {}", peer_id),
            e => panic!("{:?}", e),
        }
    }

    // ------------------------------------------------------
    // network receiving from the application
    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::StartListening { addr, sender } => {
                let _ = match self.swarm.listen_on(addr) {
                    Ok(_) => sender.send(Ok(())),
                    Err(e) => sender.send(Err(Box::new(e))),
                };
            }
            Command::Dial {
                peer_id,
                peer_addr,
                sender,
            } => {
                if self.pending_dial.contains_key(&peer_id) {
                    todo!("Already dialing peer.");
                } else {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, peer_addr.clone());
                    match self
                        .swarm
                        .dial(peer_addr.with(Protocol::P2p(peer_id.into())))
                    {
                        Ok(()) => {
                            self.pending_dial.insert(peer_id, sender);
                        }
                        Err(e) => {
                            let _ = sender.send(Err(Box::new(e)));
                        }
                    }
                }
            }
            Command::StartProviding { file_name, sender } => {
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .start_providing(file_name.into_bytes().into())
                    .expect("No store error.");
                self.pending_start_providing.insert(query_id, sender);
            }
            Command::GetProviders { file_name, sender } => {
                println!("A Sent Get Providers Event");
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .get_providers(file_name.into_bytes().into());
                self.pending_get_providers.insert(query_id, sender);
            }
            Command::GetClosestPeers { id, sender } => {
                let query_id = self.swarm.behaviour_mut().kademlia.get_closest_peers(id);
                self.pending_get_closest_peers.insert(query_id, sender);
            }
            Command::Request {
                peer,
                request,
                sender,
            } => {
                let request = serde_json::to_string(&request).unwrap();
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer, GenericRequest(request));
                self.pending_request.insert(request_id, sender);
            }
            Command::Respond { response, channel } => {
                let response = serde_json::to_string(&response).unwrap();
                self.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(channel, GenericResponse(response))
                    .expect("Connection to peer to be still open.");
            }

            Command::BootRoot { up_root, sender } => {
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .start_providing(up_root.into_bytes().into())
                    .expect("No store error.");
                self.pending_start_providing.insert(query_id, sender);
            }
            Command::Subscribe { topic } => {
                self.swarm.behaviour_mut().gossipsub.subscribe(&topic);
            }
            Command::Publish {
                topic,
                size,
                sender,
            } => {
                let size = serde_json::to_string(&size).unwrap();
                let message_id = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), size);
                self.pending_publish.insert(message_id.unwrap(), sender);
            }
        }
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "ComposedEvent")]
struct ComposedBehaviour {
    request_response: RequestResponse<GenericExchangeCodec>,
    kademlia: Kademlia<MemoryStore>,
    mdns: Mdns,
    gossipsub: gossipsub::Gossipsub,
}

#[derive(Debug)]
enum ComposedEvent {
    RequestResponse(RequestResponseEvent<GenericRequest, GenericResponse>),
    Kademlia(KademliaEvent),
    Mdns(MdnsEvent),
    Gossipsub(GossipsubEvent),
}

impl From<RequestResponseEvent<GenericRequest, GenericResponse>> for ComposedEvent {
    fn from(event: RequestResponseEvent<GenericRequest, GenericResponse>) -> Self {
        ComposedEvent::RequestResponse(event)
    }
}

impl From<KademliaEvent> for ComposedEvent {
    fn from(event: KademliaEvent) -> Self {
        ComposedEvent::Kademlia(event)
    }
}

impl From<MdnsEvent> for ComposedEvent {
    fn from(event: MdnsEvent) -> Self {
        ComposedEvent::Mdns(event)
    }
}
impl From<GossipsubEvent> for ComposedEvent {
    fn from(event: GossipsubEvent) -> Self {
        ComposedEvent::Gossipsub(event)
    }
}
//#[derive(Debug)]

enum Command {
    StartListening {
        addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: oneshot::Sender<Result<(), Box<dyn Error + Send>>>,
    },
    StartProviding {
        file_name: String,
        sender: oneshot::Sender<()>,
    },
    GetProviders {
        file_name: String,
        sender: oneshot::Sender<HashSet<PeerId>>,
    },
    GetClosestPeers {
        id: PeerId,
        sender: oneshot::Sender<Vec<PeerId>>,
    },
    Request {
        peer: PeerId,
        request: GeneralRequest,
        sender: oneshot::Sender<Result<String, Box<dyn Error + Send>>>,
    },
    Respond {
        response: GeneralResponse,
        channel: ResponseChannel<GenericResponse>,
    },
    BootRoot {
        up_root: String,
        sender: oneshot::Sender<()>,
    },
    Publish {
        topic: Topic,
        size: usize,
        sender: oneshot::Sender<(PeerId, GossipsubMessage)>,
    },
    Subscribe {
        topic: Topic,
    },
}

#[derive(Debug)]
pub enum Event {
    InboundRequest {
        request: String,
        channel: ResponseChannel<GenericResponse>,
    },
}

// Simple file exchange protocol

#[derive(Debug, Clone)]
struct GenericProtocol();
#[derive(Clone)]
struct GenericExchangeCodec();
#[derive(Debug, Clone, PartialEq, Eq)]
struct GenericRequest(String);
#[derive(Debug, Clone, PartialEq, Eq)]

pub struct GenericResponse(String);
// change the type from String to MyType

impl ProtocolName for GenericProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/lease-exchange/1".as_bytes()
    }
}

#[async_trait]
impl RequestResponseCodec for GenericExchangeCodec {
    type Protocol = GenericProtocol;
    type Request = GenericRequest;
    type Response = GenericResponse;

    async fn read_request<T>(
        &mut self,
        _: &GenericProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = read_length_prefixed(io, 1_000_000).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        Ok(GenericRequest(String::from_utf8(vec).unwrap()))
    }

    async fn read_response<T>(
        &mut self,
        _: &GenericProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = read_length_prefixed(io, 1_000_000).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        Ok(GenericResponse(String::from_utf8(vec).unwrap()))
    }

    async fn write_request<T>(
        &mut self,
        _: &GenericProtocol,
        io: &mut T,
        GenericRequest(data): GenericRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_length_prefixed(io, data).await?;
        io.close().await?;

        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &GenericProtocol,
        io: &mut T,
        GenericResponse(data): GenericResponse,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_length_prefixed(io, data).await?;
        io.close().await?;

        Ok(())
    }
}
