use async_trait::async_trait;
use consistent_hash_ring::Ring;
use futures::TryFutureExt;
use libp2p::core::upgrade::{read_length_prefixed, write_length_prefixed};
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt, StreamExt};
use libp2p::ping::Event;
use libp2p::request_response::{ProtocolName, ProtocolSupport, RequestId};
use libp2p::swarm::{keep_alive, DialError, NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{identity, mdns, multiaddr, ping, request_response, PeerId, TransportError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::io;
use tokio::select;
use tokio::sync::mpsc::{self, channel, Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use void;

#[derive(Error, Debug)]
pub enum KVServerError {
    #[error("data store disconnected")]
    Disconnect(#[from] io::Error),
    #[error("network listen error")]
    NetworkListen(#[from] multiaddr::Error),
    #[error("network dial error")]
    NetworkDial(#[from] DialError),
    #[error("network transport error")]
    NetworkTransport(#[from] TransportError<std::io::Error>),
    #[error("channel error")]
    ChannelRecv(#[from] oneshot::error::RecvError),
    #[error("channel error")]
    ChannelSend(#[from] mpsc::error::SendError<(KVRequestType, oneshot::Sender<KVResponseType>)>),
    #[error("unknown kv store error")]
    Unknown,
}

pub struct DistKVServer {
    local_command_tx: Sender<(KVRequestType, oneshot::Sender<KVResponseType>)>,
    local_command_rx: Receiver<(KVRequestType, oneshot::Sender<KVResponseType>)>,
    remote_command_rx: Receiver<(PeerId, KVRequestType, oneshot::Sender<KVResponseType>)>,
    kv_server: Arc<RwLock<KVServer>>,
    in_flight: HashMap<RequestId, (Instant, oneshot::Sender<KVResponseType>)>,
    swarm: Swarm<Behaviour>,
}

impl DistKVServer {
    fn command_tx_channel(&self) -> Sender<(KVRequestType, oneshot::Sender<KVResponseType>)> {
        self.local_command_tx.clone()
    }

    pub fn new() -> Result<Self, KVServerError> {
        let (tx, rx) = channel::<(KVRequestType, oneshot::Sender<KVResponseType>)>(1);
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());
        log::info!("Local peer id: {local_peer_id:?}");

        let transport = libp2p::tokio_development_transport(local_key)?;

        let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

        let mut swarm = Swarm::with_tokio_executor(
            transport,
            Behaviour {
                ping: ping::Behaviour::default(),
                keep_alive: keep_alive::Behaviour::default(),
                mdns,
                request_response: request_response::Behaviour::new(
                    KeyValueCodec(),
                    iter::once((KeyValueProtocol(), ProtocolSupport::Full)),
                    Default::default(),
                ),
            },
            local_peer_id,
        );

        // Tell the swarm to listen on all interfaces and a random, OS-assigned
        // port.
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

        let (cmd_channel, kv_server) = KVServer::new(local_peer_id);
        let kv_server = Arc::new(RwLock::new(kv_server));
        let in_flight: HashMap<RequestId, (Instant, oneshot::Sender<KVResponseType>)> =
            HashMap::new();

        Ok(Self {
            kv_server,
            in_flight,
            swarm,
            local_command_tx: tx,
            local_command_rx: rx,
            remote_command_rx: cmd_channel,
        })
    }

    pub fn client(&self) -> DistKVClient {
        self.into()
    }

    pub async fn run(&mut self) -> Result<(), KVServerError> {
        loop {
            select! {
                Some((request, response_tx)) = self.local_command_rx.recv() => {
                    let kv_server = self.kv_server.clone();
                    tokio::spawn(async move {

                        match request {
                            KVRequestType::Get(key) => {
                                let res = kv_server.read().await.get(&key).await;
                                response_tx.send(res).unwrap();
                            }
                            KVRequestType::Put(key, value) => {
                                let res = kv_server.write().await.put(&key, &value).await;
                                response_tx.send(res).unwrap();
                            }
                        }
                    });
                },
                Some((peer_id, cmd, res_chan)) = self.remote_command_rx.recv() => {
                    log::trace!("Remote Cmd: {:?}", cmd);

                    let request_id = self.swarm
                        .behaviour_mut()
                        .request_response
                        .send_request(&peer_id, KVRequest(cmd));

                    self.in_flight.insert(request_id, (Instant::now(), res_chan));
                },
                event = self.swarm.select_next_some() => match event {
                    SwarmEvent::NewListenAddr { address, .. } => log::info!("Listening on {address:?}"),
                    SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(addrs))) => {
                        for (id, addr) in addrs {
                            self.swarm.dial(addr)?;
                            log::info!("Dialed {id}");
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::RequestResponse(request_response::Event::Message { message, .. })) => match message {
                        request_response::Message::Request { request_id: _, request, channel } => {
                            let kv_server = self.kv_server.clone();
                            let mut kv_server = kv_server.write().await;

                            log::debug!("Request Received: {:?}", request);
                            let KVRequest(request) = request;
                            let res = kv_server.handle_request(request);
                            self.swarm
                                .behaviour_mut()
                                .request_response
                                .send_response(channel, KVResponse(res))
                                .expect("Connection to peer to be still open.");
                        }
                        request_response::Message::Response { request_id, response } => {
                            let KVResponse(response) = response;
                            let (ts, response_chan) = self.in_flight.remove(&request_id).unwrap();
                            let duration = ts.elapsed();
                            log::debug!("Response Received: {:?} {:?} ({}µs)", request_id, response, duration.as_micros());
                            response_chan.send(response).unwrap();
                        },
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Ping(Event {peer, result})) => log::debug!("Ping from: {:?} - {:?}", peer, result),
                    SwarmEvent::Behaviour(event) => log::debug!("Unknown Behaviour Event: {event:?}"),
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        let kv_server = self.kv_server.clone();
                        let mut kv_server = kv_server.write().await;
                        log::info!("Connection Established: {:?}", peer_id);
                        kv_server.add_peer(peer_id);
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        let kv_server = self.kv_server.clone();
                        let mut kv_server = kv_server.write().await;
                        log::info!("Connection Closed: {:?}", peer_id);
                        kv_server.remove_peer(peer_id);
                    }
                    event => log::debug!("Unknown Event: {event:?}"),
                    // _ => {}
                }
            }
        }
    }
}

struct KVServer {
    data: HashMap<String, String>,
    local_peer_id: PeerId,
    ring: Ring<PeerId>,
    cmd_channel: Sender<(PeerId, KVRequestType, oneshot::Sender<KVResponseType>)>,
}

impl KVServer {
    fn new(
        local_peer_id: PeerId,
    ) -> (
        Receiver<(PeerId, KVRequestType, oneshot::Sender<KVResponseType>)>,
        Self,
    ) {
        let mut ring = consistent_hash_ring::RingBuilder::default().build();
        ring.insert(local_peer_id);
        let (cmd_send, cmd_recv) = channel(1);

        let res = Self {
            ring,
            local_peer_id,
            cmd_channel: cmd_send,
            data: HashMap::new(),
        };

        (cmd_recv, res)
    }

    fn handle_request(&mut self, request: KVRequestType) -> KVResponseType {
        match request {
            KVRequestType::Get(k) => {
                let value = self.data.get(&k);
                KVResponseType::Result(value.cloned())
            }
            KVRequestType::Put(k, v) => {
                self.data.insert(k, v);
                KVResponseType::Ok
            }
        }
    }

    async fn get(&self, key: &str) -> KVResponseType {
        let node = self.ring.get(key);

        if node == &self.local_peer_id {
            let res = self.data.get(key).cloned();
            KVResponseType::Result(res)
        } else {
            let (remote_tx, remote_rx) = oneshot::channel();
            let req = KVRequestType::Get(key.to_string());
            log::trace!("{:?} To: {:?}", req, node);
            self.cmd_channel
                .send((*node, req.clone(), remote_tx))
                .await
                .unwrap();
            log::trace!("{:?} Sent", req.clone());
            let res = remote_rx.await.unwrap();
            log::trace!("{:?} Res: {:?}", req, res);
            res
        }
    }

    async fn put(&mut self, key: &str, value: &str) -> KVResponseType {
        let node = self.ring.get(key);

        if node == &self.local_peer_id {
            let _ = self.data.insert(key.to_string(), value.to_string());
            KVResponseType::Ok
        } else {
            let (remote_tx, remote_rx) = oneshot::channel();
            let req = KVRequestType::Put(key.to_string(), value.to_string());
            log::trace!("{:?} To: {:?}", req, node);
            self.cmd_channel
                .send((*node, req.clone(), remote_tx))
                .await
                .unwrap();
            log::trace!("{:?} Sent", req.clone());
            let res = remote_rx.await.unwrap();
            log::trace!("{:?} Res: {:?}", req, res);
            res
        }
    }

    fn add_peer(&mut self, peer: PeerId) {
        self.ring.insert(peer);
        log::debug!(
            "Ring: len: {}, vnodes: {}",
            self.ring.len(),
            self.ring.vnodes()
        );
    }

    fn remove_peer(&mut self, peer: PeerId) {
        self.ring.remove(&peer);
        log::debug!(
            "Ring: len: {}, vnodes: {}",
            self.ring.len(),
            self.ring.vnodes()
        );
    }
}

pub struct DistKVClient {
    local_command_tx: Sender<(KVRequestType, oneshot::Sender<KVResponseType>)>,
}

impl DistKVClient {
    pub fn new(local_command_tx: Sender<(KVRequestType, oneshot::Sender<KVResponseType>)>) -> Self {
        Self { local_command_tx }
    }

    pub fn send(
        &self,
        request: KVRequestType,
    ) -> impl std::future::Future<Output = Result<KVResponseType, KVServerError>> + '_ {
        let (tx, rx) = oneshot::channel();
        self.local_command_tx
            .send((request, tx))
            .map_err(|e| e.into())
            .and_then(|_| rx.map_err(|e| e.into()))
    }
}

impl From<&DistKVServer> for DistKVClient {
    fn from(kv_server: &DistKVServer) -> Self {
        DistKVClient::new(kv_server.command_tx_channel())
    }
}

/// Our network behaviour.
///
/// For illustrative purposes, this includes the [`KeepAlive`](behaviour::KeepAlive) behaviour so a continuous sequence of
/// pings can be observed.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "MyBehaviourEvent")]
struct Behaviour {
    keep_alive: keep_alive::Behaviour,
    ping: ping::Behaviour,
    mdns: mdns::tokio::Behaviour,
    request_response: request_response::Behaviour<KeyValueCodec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KVRequestType {
    Get(String),
    Put(String, String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KVResponseType {
    Error(String),
    Result(Option<String>),
    Ok,
}

#[derive(Debug, Clone)]
struct KeyValueProtocol();
#[derive(Clone)]
struct KeyValueCodec();
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct KVRequest(KVRequestType);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KVResponse(KVResponseType);

impl ProtocolName for KeyValueProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/key-value/1".as_bytes()
    }
}

#[async_trait]
impl request_response::Codec for KeyValueCodec {
    type Protocol = KeyValueProtocol;
    type Request = KVRequest;
    type Response = KVResponse;

    async fn read_request<T>(
        &mut self,
        _: &KeyValueProtocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = read_length_prefixed(io, 1_000_000).await?;

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        let request: KVRequest = bincode::deserialize(&vec[..]).unwrap();

        Ok(request)
    }

    async fn read_response<T>(
        &mut self,
        _: &KeyValueProtocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let vec = read_length_prefixed(io, 500_000_000).await?; // update transfer maximum

        if vec.is_empty() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        let response: KVResponse = bincode::deserialize(&vec[..]).unwrap();

        Ok(response)
    }

    async fn write_request<T>(
        &mut self,
        _: &KeyValueProtocol,
        io: &mut T,
        KVRequest(data): KVRequest,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let encoded = bincode::serialize(&data).unwrap();
        write_length_prefixed(io, encoded).await?;
        io.close().await?;

        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &KeyValueProtocol,
        io: &mut T,
        KVResponse(data): KVResponse,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let encoded = bincode::serialize(&data).unwrap();
        write_length_prefixed(io, encoded).await?;
        io.close().await?;

        Ok(())
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum MyBehaviourEvent {
    Mdns(mdns::Event),
    Ping(ping::Event),
    Void,
    RequestResponse(request_response::Event<KVRequest, KVResponse>),
}

impl From<mdns::Event> for MyBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        MyBehaviourEvent::Mdns(event)
    }
}

impl From<ping::Event> for MyBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        MyBehaviourEvent::Ping(event)
    }
}

impl From<void::Void> for MyBehaviourEvent {
    fn from(_: void::Void) -> Self {
        MyBehaviourEvent::Void
    }
}

impl From<request_response::Event<KVRequest, KVResponse>> for MyBehaviourEvent {
    fn from(event: request_response::Event<KVRequest, KVResponse>) -> Self {
        MyBehaviourEvent::RequestResponse(event)
    }
}
