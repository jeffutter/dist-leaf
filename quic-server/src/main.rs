use bytes::Bytes;
use consistent_hash_ring::{Ring, RingBuilder};
use env_logger::Env;
use mdns_sd::{Receiver, ServiceDaemon, ServiceEvent, ServiceInfo};
use net::{KVRequestType, KVResponseType};
use s2n_quic::{client::Connect, connection, stream::BidirectionalStream, Client, Server};
use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    thread,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    runtime, select,
    sync::{mpsc, oneshot, Mutex},
    task,
    time::{self, Instant},
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
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("unknown server error")]
    Unknown,
}

struct VNode {
    connections: Arc<Mutex<S2SConnections>>,
    storage: db::Database,
    socket_addr: SocketAddr,
    mdns_receiver: Receiver<ServiceEvent>,
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
    ) -> Result<Self, Box<dyn Error>> {
        let vnode_id = VNodeId::new(node_id, core_id);
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");

        let service_type = "_mdns-quic-db._udp.local.";
        let receiver = mdns.browse(service_type).expect("Failed to browse");

        let server = Server::builder()
            .with_tls((CERT_PEM, KEY_PEM))?
            .with_io("0.0.0.0:0")?
            .start()?;

        // node-id and core-id is too long
        let instance_name = format!("node-{}", core_id);
        let host_ipv4: Ipv4Addr = local_ip;
        let host_name = format!("{}.local.", host_ipv4);
        let port = server.local_addr()?.port();
        log::info!("Starting Server on Port: {}", port);
        let properties = [
            ("node_id".to_string(), node_id.to_string()),
            ("core_id".to_string(), core_id.to_string()),
        ];
        log::debug!("Properties: {:?}", properties);
        let my_service = ServiceInfo::new(
            service_type,
            &instance_name,
            &host_name,
            host_ipv4,
            port,
            &properties[..],
        )
        .unwrap();

        mdns.register(my_service.clone())
            .expect("Failed to register our service");

        let socket_addr: SocketAddr = format!(
            "{}:{}",
            my_service.get_addresses().iter().next().unwrap(),
            my_service.get_port()
        )
        .parse()
        .unwrap();

        let client = Client::builder()
            .with_tls(CERT_PEM)?
            .with_io("0.0.0.0:0")?
            .start()?;

        let mut connections = S2SConnections::new(client, vnode_id);
        for (vnode_id, tx) in vnode_to_tx {
            connections.add_channel(vnode_id, tx)
        }
        let connections = Arc::new(Mutex::new(connections));
        let storage = db::Database::new_tmp();

        Ok(Self {
            connections,
            storage,
            socket_addr,
            server,
            mdns_receiver: receiver,
            rx,
        })
    }

    fn mdns_loop(
        connections: Arc<Mutex<S2SConnections>>,
        local_socket_addr: SocketAddr,
        mdns_receiver: Receiver<ServiceEvent>,
    ) -> impl Future<Output = Result<(), ServerError>> {
        async move {
            let mut interval = time::interval(Duration::from_secs(30));

            loop {
                interval.tick().await;
                while let Ok(event) = mdns_receiver.recv_async().await {
                    match event {
                        ServiceEvent::ServiceResolved(info) => {
                            let node_id: Uuid = info
                                .get_property_val_str("node_id")
                                .unwrap()
                                .parse()
                                .unwrap();
                            let core_id: Uuid = info
                                .get_property_val_str("core_id")
                                .unwrap()
                                .parse()
                                .unwrap();
                            let vnode_id = VNodeId::new(node_id, core_id);

                            log::debug!(
                                "Resolved a new service: {} {}",
                                info.get_fullname(),
                                info.get_port()
                            );

                            let socket_addr: SocketAddr = SocketAddr::new(
                                info.get_addresses()
                                    .iter()
                                    .next()
                                    .unwrap()
                                    .to_owned()
                                    .into(),
                                info.get_port(),
                            );

                            if socket_addr != local_socket_addr {
                                connections
                                    .lock()
                                    .await
                                    .add_connection(vnode_id, socket_addr)
                                    .await?;
                            }
                        }
                        ServiceEvent::SearchStarted(_) => (),
                        other_event => {
                            log::debug!("Received other event: {:?}", &other_event);
                        }
                    }
                }
            }
        }
    }

    fn handle_stream(
        stream: BidirectionalStream,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            let (mut receive_stream, mut send_stream) = stream.split();
            let mut data_stream = net::ProtoReader::new();

            while let Some(data) = receive_stream.receive().await.unwrap() {
                data_stream.add_data(data);
                if let Some(data) = data_stream.read_message() {
                    let start = Instant::now();
                    log::debug!("Received Data: {:?}", data);
                    let req = net::decode_request(data.as_ref()).unwrap();
                    log::debug!(
                        "decoded: {}µs",
                        Instant::now().duration_since(start).as_micros()
                    );

                    match connections.lock().await.get(req.key()) {
                        Destination::Remote(connection) => {
                            log::debug!("Forward to: {:?}", connection);
                            log::debug!("Forwarding Data:     {:?}", data);
                            connection.send(data).await.unwrap();
                            let data = connection.recv().await.unwrap().unwrap();
                            send_stream.send(data).await.expect("stream should be open");
                        }
                        Destination::Adjacent(channel) => {
                            log::debug!("Forward to Adjacent");
                            log::debug!("Forwarding Data:     {:?}", data);
                            let (tx, rx) = oneshot::channel();
                            channel.send((data, tx)).await.unwrap();
                            let data = rx.await.unwrap();
                            send_stream.send(data).await.expect("stream should be open");
                        }
                        Destination::Local => match req {
                            KVRequestType::Get(key) => {
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
                                let res = net::encode_response(KVResponseType::Result(res_data));
                                log::debug!(
                                    "encoded: {}µs",
                                    Instant::now().duration_since(start).as_micros()
                                );
                                send_stream.send(res).await.expect("stream should be open");
                                log::debug!(
                                    "sent: {}µs",
                                    Instant::now().duration_since(start).as_micros()
                                );
                            }
                            KVRequestType::Put(key, value) => {
                                storage.put(&key, &value).unwrap();
                                let res = net::encode_response(KVResponseType::Ok);
                                send_stream.send(res).await.expect("stream should be open");
                            }
                        },
                    }
                }
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

    async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        let storage = self.storage.clone();
        let mdns_receiver = self.mdns_receiver.clone();
        let local_socket_addr = self.socket_addr;

        let _mdns_search = task::spawn(Self::mdns_loop(
            self.connections.clone(),
            local_socket_addr,
            mdns_receiver,
        ));

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
                        KVRequestType::Get(key) => {
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
                            let res = net::encode_response(KVResponseType::Result(res_data));
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
                        KVRequestType::Put(key, value) => {
                            storage.put(&key, &value).unwrap();
                            let res = net::encode_response(KVResponseType::Ok);
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
    env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();

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
                        let mut vnode =
                            VNode::new(node_id, core_id, local_ip, rx, core_to_tx).unwrap();

                        vnode.run().await.unwrap();
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
