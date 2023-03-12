use async_trait::async_trait;
use consistent_hash_ring::Ring;
use futures::Future;
use libp2p::core::upgrade::{read_length_prefixed, write_length_prefixed};
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt, StreamExt};
use libp2p::ping::Event;
use libp2p::request_response::{ProtocolName, ProtocolSupport, RequestId};
use libp2p::swarm::{keep_alive, NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{identity, mdns, ping, request_response, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::iter;
use std::sync::Arc;
use std::time::Instant;
use tokio::io;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
use void;

pub fn start() -> (
    Sender<(KVRequestType, oneshot::Sender<KVResponseType>)>,
    impl Future<Output = Result<(), Box<dyn Error>>>,
) {
    let (tx, rx) = channel::<(KVRequestType, oneshot::Sender<KVResponseType>)>(1);

    let future = main(rx);

    (tx, future)
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

pub async fn main(
    mut cli_cmd: Receiver<(KVRequestType, oneshot::Sender<KVResponseType>)>,
) -> Result<(), Box<dyn Error>> {
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

    let (mut cmd_channel, kv_server) = KVServer::new(local_peer_id);
    let kv_server = Arc::new(Mutex::new(kv_server));
    let mut in_flight: HashMap<RequestId, (Instant, oneshot::Sender<KVResponseType>)> =
        HashMap::new();

    let kv_server1 = kv_server.clone();
    let _h1 = tokio::spawn(async move {
        loop {
            let (request, response_tx) = cli_cmd.recv().await.unwrap();
            let mut kv_server = kv_server1.lock().await;

            match request {
                KVRequestType::Get(key) => {
                    let res = kv_server.get(&key).await;
                    response_tx.send(res).unwrap();
                }
                KVRequestType::Put(key, value) => {
                    let res = kv_server.put(&key, &value).await;
                    response_tx.send(res).unwrap();
                }
            }
        }
    });

    loop {
        select! {
            Some((peer_id, cmd, res_chan)) = cmd_channel.recv() => {
                log::trace!("Remote Cmd: {:?}", cmd);

                let request_id = swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer_id, KVRequest(cmd));

                in_flight.insert(request_id, (Instant::now(), res_chan));
            },
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => log::info!("Listening on {address:?}"),
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(addrs))) => {
                    for (id, addr) in addrs {
                        swarm.dial(addr)?;
                        log::info!("Dialed {id}");
                    }
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::RequestResponse(request_response::Event::Message { message, .. })) => match message {
                    request_response::Message::Request { request_id: _, request, channel } => {
                        log::debug!("Request Received: {:?}", request);
                        let mut kv_server = kv_server.lock().await;
                        let KVRequest(request) = request;
                        let res = kv_server.handle_request(request);
                        swarm
                            .behaviour_mut()
                            .request_response
                            .send_response(channel, KVResponse(res))
                            .expect("Connection to peer to be still open.");
                    }
                    request_response::Message::Response { request_id, response } => {
                        let KVResponse(response) = response;
                        let (ts, response_chan) = in_flight.remove(&request_id).unwrap();
                        let duration = ts.elapsed();
                        log::debug!("Response Received: {:?} {:?} ({}µs)", request_id, response, duration.as_micros());
                        response_chan.send(response).unwrap();
                    },
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::Ping(Event {peer, result})) => log::debug!("Ping from: {:?} - {:?}", peer, result),
                SwarmEvent::Behaviour(event) => log::debug!("Unknown Behaviour Event: {event:?}"),
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    log::info!("Connection Established: {:?}", peer_id);
                    let mut kv_server = kv_server.lock().await;
                    kv_server.add_peer(peer_id);
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    log::info!("Connection Closed: {:?}", peer_id);
                    let mut kv_server = kv_server.lock().await;
                    kv_server.remove_peer(peer_id);
                }
                event => log::debug!("Unknown Event: {event:?}"),
                // _ => {}
            }
        }
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
