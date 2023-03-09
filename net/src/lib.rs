use async_trait::async_trait;
use consistent_hash_ring::Ring;
use libp2p::core::upgrade::{read_length_prefixed, write_length_prefixed};
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt, StreamExt};
use libp2p::request_response::{ProtocolName, ProtocolSupport, RequestId};
use libp2p::swarm::{keep_alive, NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{identity, mdns, ping, request_response, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::iter;
use std::time::Instant;
use tokio::io::{self, AsyncBufReadExt};
use tokio::select;
use tokio::sync::mpsc::channel;
use void;

pub async fn main() -> Result<(), Box<dyn Error>> {
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("Local peer id: {local_peer_id:?}");

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

    let mut ring = consistent_hash_ring::RingBuilder::default().build();
    ring.insert(local_peer_id);

    let mut data: HashMap<String, String> = HashMap::new();
    let mut in_flight: HashMap<RequestId, Instant> = HashMap::new();

    let (tx, mut rx) = channel(1);

    tokio::spawn(async move {
        let mut stdin = io::BufReader::new(io::stdin()).lines();
        loop {
            let next_line = stdin
                .next_line()
                .await
                .unwrap()
                .expect("Stdin not to close");
            if let Err(_) = tx.send(next_line).await {
                println!("receiver dropped");
                return;
            }
        }
    });

    loop {
        select! {
            line = rx.recv() => handle_input_line(&ring, local_peer_id, &mut data, &mut in_flight, swarm.behaviour_mut(), line.unwrap()),
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => println!("Listening on {address:?}"),
                SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(addrs))) => {
                    for (id, addr) in addrs {
                        swarm.dial(addr)?;
                        println!("Dialed {id}");
                    }
                }
                SwarmEvent::Behaviour(MyBehaviourEvent::RequestResponse(request_response::Event::Message { message, .. })) => match message {
                    request_response::Message::Request { request_id: _, request, channel } => match request {
                        KVRequest(KVRequestType::Get(k)) => {
                            let value = data.get(&k);
                            swarm
                                .behaviour_mut()
                                .request_response
                                .send_response(channel, KVResponse(KVResponseType::Result(value.cloned())))
                                .expect("Connection to peer to be still open.");
                        },
                        KVRequest(KVRequestType::Put(k, v)) => {
                            data.insert(k, v);
                            swarm
                                .behaviour_mut()
                                .request_response
                                .send_response(channel, KVResponse(KVResponseType::Ok))
                                .expect("Connection to peer to be still open.");
                        }

                    }
                    request_response::Message::Response { request_id, response } => match response {
                        KVResponse(KVResponseType::Ok) => {
                            let start_time = in_flight.remove(&request_id).unwrap();
                            let duration = Instant::now().duration_since(start_time);
                            println!("OK! = {:?}", duration);
                        }
                        KVResponse(KVResponseType::Result(v)) => {
                            let start_time = in_flight.remove(&request_id).unwrap();
                            let duration = Instant::now().duration_since(start_time);
                            println!("Result: {:?} - {:?}", v, duration);
                        }
                        KVResponse(KVResponseType::Error(e)) => {
                            let start_time = in_flight.remove(&request_id).unwrap();
                            let duration = Instant::now().duration_since(start_time);
                            println!("Error: {:?} = {:?}", e, duration);
                        }
                    },
                }
                SwarmEvent::Behaviour(event) => println!("{event:?}"),
                SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    ring.insert(peer_id);
                    println!("Ring: len: {}, vnodes: {}", ring.len(), ring.vnodes());
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    ring.remove(&peer_id);
                    println!("Ring: len: {}, vnodes: {}", ring.len(), ring.vnodes());
                }
                event => println!("{event:?}"),
                // _ => {}
            }
        }
    }
}

fn handle_input_line(
    ring: &Ring<PeerId>,
    local_peer_id: PeerId,
    data: &mut HashMap<String, String>,
    in_flight: &mut HashMap<RequestId, Instant>,
    behaviour: &mut Behaviour,
    line: String,
) {
    let mut args = line.split(' ');

    let next = args.next().map(|x| x.to_uppercase());

    match next.as_deref() {
        Some("GET") => {
            let key = {
                match args.next() {
                    Some(key) => key,
                    None => {
                        println!("Expected key");
                        return;
                    }
                }
            };
            let node = ring.get(key);
            if node == &local_peer_id {
                println!("Local Get: {:?}", data.get(key));
            } else {
                println!("Get Node For '{}': {:?}", key, node);
                let request_id = behaviour
                    .request_response
                    .send_request(&node, KVRequest(KVRequestType::Get(key.to_string())));
                in_flight.insert(request_id, Instant::now());
                println!("Request Sent: {:?}", request_id);
            }
        }
        Some("PUT") => {
            let key = {
                match args.next() {
                    Some(key) => key,
                    None => {
                        println!("Expected key");
                        return;
                    }
                }
            };
            let value = {
                match args.next() {
                    Some(value) => value,
                    None => {
                        println!("Expected value");
                        return;
                    }
                }
            };
            let node = ring.get(key);
            if node == &local_peer_id {
                println!(
                    "Local Put: {:?}",
                    data.insert(key.to_string(), value.to_string())
                );
            } else {
                println!("Put Node For '{}={}': {:?}", key, value, node);
                let request_id = behaviour.request_response.send_request(
                    &node,
                    KVRequest(KVRequestType::Put(key.to_string(), value.to_string())),
                );
                in_flight.insert(request_id, Instant::now());
                println!("Request Sent: {:?}", request_id);
            }
        }
        _ => {
            println!("expected GET, PUT");
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
enum KVRequestType {
    Get(String),
    Put(String, String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum KVResponseType {
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
