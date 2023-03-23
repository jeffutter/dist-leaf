mod mdns;

use bytes::Bytes;
use consistent_hash_ring::{Ring, RingBuilder};
use env_logger::Env;
use futures::StreamExt;
use net::{KVRequestType, KVResponseType};
use quic_transport::{DataStream, RequestStream};
use s2n_quic::{client::Connect, connection, stream::BidirectionalStream, Client, Server};
use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    thread,
};
use thiserror::Error;
use tokio::{
    runtime, select,
    sync::{mpsc, oneshot, Mutex},
    time::Instant,
};
use uuid::Uuid;

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));
/// NOTE: this certificate is to be used for demonstration purposes only!
pub static KEY_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/key.pem"));

#[derive(Clone, Eq, Hash, PartialEq)]
struct VNodeId {
    node_id: Uuid,
    core_id: Uuid,
}

impl VNodeId {
    fn new(node_id: Uuid, core_id: Uuid) -> Self {
        Self { node_id, core_id }
    }
}

#[derive(Debug)]
struct S2SConnection {
    stream: BidirectionalStream,
}

impl S2SConnection {
    async fn new(addr: SocketAddr, client: Client) -> Result<Self, ServerError> {
        let connect = Connect::new(addr).with_server_name("localhost");
        let mut connection = client.connect(connect).await?;
        connection.keep_alive(true)?;
        let stream = connection.open_bidirectional_stream().await?;

        Ok(Self { stream })
    }

    async fn send(&mut self, data: Bytes) -> Result<(), Box<dyn Error>> {
        self.stream.send(data).await?;
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Bytes>, Box<dyn Error>> {
        let data = self.stream.receive().await?;
        Ok(data)
    }
}

struct S2SConnections {
    connections: HashMap<VNodeId, S2SConnection>,
    channels: HashMap<VNodeId, mpsc::Sender<(Bytes, oneshot::Sender<Bytes>)>>,
    ring: Ring<VNodeId>,
    client: Client,
    vnode_id: VNodeId,
}

impl S2SConnections {
    fn new(client: Client, vnode_id: VNodeId) -> Self {
        let mut ring = RingBuilder::default().build();
        ring.insert(vnode_id.clone());

        Self {
            connections: HashMap::new(),
            channels: HashMap::new(),
            ring,
            client,
            vnode_id,
        }
    }

    async fn add_connection(
        &mut self,
        vnode_id: VNodeId,
        addr: SocketAddr,
    ) -> Result<(), ServerError> {
        if let None = self.connections.get(&vnode_id) {
            let s2s_connection = S2SConnection::new(addr, self.client.clone()).await?;
            self.connections.insert(vnode_id.clone(), s2s_connection);
            self.ring.insert(vnode_id);
        }

        Ok(())
    }

    fn add_channel(
        &mut self,
        vnode_id: VNodeId,
        channel: mpsc::Sender<(Bytes, oneshot::Sender<Bytes>)>,
    ) {
        if let None = self.channels.get(&vnode_id) {
            self.channels.insert(vnode_id.clone(), channel);
            self.ring.insert(vnode_id);
        }
    }

    fn get(&mut self, key: &str) -> Destination {
        let vnode_id = self.ring.get(key);

        match vnode_id {
            vnode_id if vnode_id == &self.vnode_id => Destination::Local,
            VNodeId {
                node_id,
                core_id: _,
            } if node_id == &self.vnode_id.node_id => {
                let channel = self.channels.get_mut(vnode_id).unwrap();
                Destination::Adjacent(channel)
            }
            vnode_id => {
                let connection = self.connections.get_mut(vnode_id).unwrap();
                Destination::Remote(connection)
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("decoding error")]
    Decoding(#[from] net::KVServerError),
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("stream error")]
    Stream(#[from] s2n_quic::stream::Error),
    #[error("unknown server error")]
    Unknown,
    #[error("initialization error: {}", .0)]
    Initialization(String),
}

struct VNode {
    connections: Arc<Mutex<S2SConnections>>,
    storage: db::Database,
    mdns: mdns::MDNS,
    server: s2n_quic::Server,
    rx: mpsc::Receiver<(Bytes, oneshot::Sender<Bytes>)>,
}

impl VNode {
    fn new(
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        rx: mpsc::Receiver<(Bytes, oneshot::Sender<Bytes>)>,
        vnode_to_tx: HashMap<VNodeId, mpsc::Sender<(Bytes, oneshot::Sender<Bytes>)>>,
    ) -> Result<Self, ServerError> {
        let vnode_id = VNodeId::new(node_id, core_id);

        let server = Server::builder()
            .with_tls((CERT_PEM, KEY_PEM))
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        // node-id and core-id is too long
        let port = server
            .local_addr()
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .port();
        log::info!("Starting Server on Port: {}", port);

        let client = Client::builder()
            .with_tls(CERT_PEM)
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        let mut connections = S2SConnections::new(client, vnode_id);
        for (vnode_id, tx) in vnode_to_tx {
            connections.add_channel(vnode_id, tx)
        }
        let connections = Arc::new(Mutex::new(connections));
        let storage = db::Database::new_tmp();

        let mdns = mdns::MDNS::new(node_id, core_id, connections.clone(), local_ip, port);

        Ok(Self {
            connections,
            storage,
            server,
            mdns,
            rx,
        })
    }

    fn handle_stream(
        stream: BidirectionalStream,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            let (receive_stream, mut send_stream) = stream.split();
            let data_stream = DataStream::new(receive_stream);
            let mut request_stream = RequestStream::new(data_stream);

            let (send_tx, mut send_rx): (mpsc::Sender<Bytes>, mpsc::Receiver<Bytes>) =
                mpsc::channel(100);

            tokio::spawn(async move {
                while let Some(data) = send_rx.recv().await {
                    send_stream.send(data).await.unwrap();
                }
            });

            while let Some(Ok((req, data))) = request_stream.next().await {
                let storage = storage.clone();
                let connections = connections.clone();
                let send_tx = send_tx.clone();
                tokio::spawn(async move {
                    let start = Instant::now();

                    match connections.lock().await.get(req.key()) {
                        Destination::Remote(connection) => {
                            log::debug!("Forward to: {:?}", connection);
                            log::debug!("Forwarding Data:     {:?}", req);
                            connection.send(data).await.unwrap();
                            let data = connection.recv().await.unwrap().unwrap();
                            send_tx.send(data).await.expect("stream should be open");
                        }
                        Destination::Adjacent(channel) => {
                            log::debug!("Forward to Adjacent");
                            log::debug!("Forwarding Data:     {:?}", data);
                            let (tx, rx) = oneshot::channel();
                            channel.send((data, tx)).await.unwrap();
                            let data = rx.await.unwrap();
                            send_tx.send(data).await.expect("stream should be open");
                        }
                        Destination::Local => match req {
                            KVRequestType::Get { id, key } => {
                                log::debug!("Serve Local");
                                log::debug!(
                                    "checked connection: {}µs",
                                    Instant::now().duration_since(start).as_micros()
                                );
                                let res_data = storage.get(&key).unwrap();
                                log::debug!(
                                    "fetched: {}µs",
                                    Instant::now().duration_since(start).as_micros()
                                );
                                let res = net::encode_response(KVResponseType::Result {
                                    id,
                                    result: res_data,
                                });
                                log::debug!(
                                    "encoded: {}µs",
                                    Instant::now().duration_since(start).as_micros()
                                );
                                send_tx.send(res).await.expect("stream should be open");
                                log::debug!(
                                    "sent: {}µs",
                                    Instant::now().duration_since(start).as_micros()
                                );
                            }
                            KVRequestType::Put { id, key, value } => {
                                storage.put(&key, &value).unwrap();
                                let res = net::encode_response(KVResponseType::Ok(id));
                                send_tx.send(res).await.expect("stream should be open");
                            }
                        },
                    }
                });
            }
        }
    }

    fn handle_connection(
        mut connection: connection::Connection,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            log::debug!("New Connection");
            while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                log::debug!("New Stream");
                tokio::spawn(Self::handle_stream(
                    stream,
                    connections.clone(),
                    storage.clone(),
                ));
            }
        }
    }

    async fn run(&mut self) -> Result<(), ServerError> {
        let storage = self.storage.clone();

        loop {
            select! {
                Some(connection) = self.server.accept() => {
                    // spawn a new task for the connection
                    tokio::spawn(Self::handle_connection(
                        connection,
                        self.connections.clone(),
                        storage.clone(),
                    ));
                }
                Some((data, tx)) = self.rx.recv() => {
                    let start = Instant::now();
                    log::debug!("Handling Local Data: {:?}", data);
                    let req = net::decode_request(data.as_ref()).unwrap();
                    log::debug!(
                        "decoded: {}µs",
                        Instant::now().duration_since(start).as_micros()
                    );
                    match req {
                        KVRequestType::Get{id, key} => {
                            log::debug!("Serve Local");
                            log::debug!(
                                "checked connection: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                            let res_data = storage.get(&key).unwrap();
                            log::debug!(
                                "fetched: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                            let res = net::encode_response(KVResponseType::Result{id, result: res_data});
                            log::debug!(
                                "encoded: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                            tx.send(res).unwrap();
                            log::debug!(
                                "sent: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                        }
                        KVRequestType::Put{id, key, value} => {
                            storage.put(&key, &value).unwrap();
                            let res = net::encode_response(KVResponseType::Ok(id));
                            tx.send(res).unwrap();
                        }
                    }

                }
            }
        }
    }
}

enum Destination<'a> {
    Remote(&'a mut S2SConnection),
    Adjacent(&'a mut mpsc::Sender<(Bytes, oneshot::Sender<Bytes>)>),
    Local,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let local_ip = match local_ip_address::local_ip()? {
        std::net::IpAddr::V4(ip) => ip,
        std::net::IpAddr::V6(_) => todo!(),
    };

    let node_id = Uuid::new_v4();

    let core_ids = core_affinity::get_core_ids().unwrap();

    let (mut core_to_vnode_id, mut core_to_rx, core_to_tx): (
        HashMap<usize, VNodeId>,
        HashMap<usize, mpsc::Receiver<(Bytes, oneshot::Sender<Bytes>)>>,
        HashMap<VNodeId, mpsc::Sender<(Bytes, oneshot::Sender<Bytes>)>>,
    ) = core_ids.iter().fold(
        (HashMap::new(), HashMap::new(), HashMap::new()),
        |(mut core_to_vnode_id, mut rx_acc, mut tx_acc), id| {
            let id = id.id;
            let core_id = Uuid::new_v4();
            let vnode_id = VNodeId::new(node_id, core_id);
            let (tx, rx) = tokio::sync::mpsc::channel::<(Bytes, oneshot::Sender<Bytes>)>(1);
            core_to_vnode_id.insert(id, vnode_id.clone());
            rx_acc.insert(id, rx);
            tx_acc.insert(vnode_id, tx);
            (core_to_vnode_id, rx_acc, tx_acc)
        },
    );

    let handles = core_ids
        .into_iter()
        .map(|id| {
            let rx = core_to_rx.remove(&id.id).unwrap();
            let vnode_id = core_to_vnode_id.remove(&id.id).unwrap();
            let VNodeId { node_id, core_id } = vnode_id;
            let core_to_tx = core_to_tx.clone();
            thread::spawn(move || {
                // Pin this thread to a single CPU core.
                let res = core_affinity::set_for_current(id);
                if res {
                    let rt = runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    log::info!("Starting Thread: #{:?}", id);
                    rt.block_on(async {
                        let mut vnode = VNode::new(node_id, core_id, local_ip, rx, core_to_tx)?;

                        let _ = vnode.mdns.spawn();
                        vnode.run().await?;

                        Ok::<(), ServerError>(())
                    })
                    .unwrap();
                }

                Ok::<(), ServerError>(())
            })
        })
        .collect::<Vec<_>>();

    for handle in handles.into_iter() {
        handle.join().unwrap().unwrap();
    }

    Ok(())
}
