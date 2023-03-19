use bytes::Bytes;
use consistent_hash_ring::{Ring, RingBuilder};
use mdns_sd::{Receiver, ServiceDaemon, ServiceEvent, ServiceInfo};
use s2n_quic::{
    client::Connect, connection, stream::BidirectionalStream, Client, Connection, Server,
};
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
    runtime,
    sync::Mutex,
    task,
    time::{self, Instant},
};
use uuid::Uuid;

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));
/// NOTE: this certificate is to be used for demonstration purposes only!
pub static KEY_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/key.pem"));

#[derive(Debug)]
struct S2SConnection {
    addr: SocketAddr,
    client: Client,
    connection: Connection,
    stream: BidirectionalStream,
}

impl S2SConnection {
    async fn new(addr: SocketAddr, client: Client) -> Result<Self, ServerError> {
        let connect = Connect::new(addr).with_server_name("localhost");
        let mut connection = client.connect(connect).await?;
        connection.keep_alive(true)?;
        let stream = connection.open_bidirectional_stream().await?;

        Ok(Self {
            addr,
            client,
            connection,
            stream,
        })
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
    connections: HashMap<SocketAddr, S2SConnection>,
    ring: Ring<SocketAddr>,
    client: Client,
    local_addr: SocketAddr,
}

impl S2SConnections {
    fn new(client: Client, local_addr: SocketAddr) -> Self {
        let mut ring = RingBuilder::default().build();
        ring.insert(local_addr);

        Self {
            connections: HashMap::new(),
            ring,
            client,
            local_addr,
        }
    }

    async fn add(&mut self, addr: SocketAddr) -> Result<(), ServerError> {
        if let None = self.connections.get(&addr) {
            let s2s_connection = S2SConnection::new(addr, self.client.clone()).await?;
            self.connections.insert(addr, s2s_connection);
            self.ring.insert(addr);
        }

        Ok(())
    }

    fn get(&mut self, key: &str) -> Destination {
        let socket_addr = self.ring.get(key);
        if socket_addr == &self.local_addr {
            Destination::Local
        } else {
            let connection = self.connections.get_mut(socket_addr).unwrap();
            Destination::Remote(connection)
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
}

impl VNode {
    fn new(id: Uuid, local_ip: Ipv4Addr) -> Result<Self, Box<dyn Error>> {
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");

        let service_type = "_mdns-quic-db._udp.local.";
        let receiver = mdns.browse(service_type).expect("Failed to browse");

        let server = Server::builder()
            .with_tls((CERT_PEM, KEY_PEM))?
            .with_io("0.0.0.0:0")?
            .start()?;

        let instance_name = format!("node-{}", id);
        let host_ipv4: Ipv4Addr = local_ip;
        let host_name = format!("{}.local.", host_ipv4);
        let port = server.local_addr()?.port();
        let my_service = ServiceInfo::new(
            service_type,
            &instance_name,
            &host_name,
            host_ipv4,
            port,
            None,
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

        let connections = S2SConnections::new(client, socket_addr);
        let connections = Arc::new(Mutex::new(connections));
        let storage = db::Database::new_tmp();

        Ok(Self {
            connections,
            storage,
            socket_addr,
            server,
            mdns_receiver: receiver,
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
                            println!(
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
                                connections.lock().await.add(socket_addr).await?;
                            }
                        }
                        other_event => {
                            println!("Received other event: {:?}", &other_event);
                        }
                    }
                }
            }
        }
    }

    fn handle_stream(
        mut stream: BidirectionalStream,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            while let Ok(Some(data)) = stream.receive().await {
                println!("New data");
                let start = Instant::now();
                let req = net::decode_request(data.as_ref()).unwrap();
                println!(
                    "decoded: {}µs",
                    Instant::now().duration_since(start).as_micros()
                );

                match req {
                    net::KVRequestType::Get(key) => match connections.lock().await.get(&key) {
                        Destination::Remote(connection) => {
                            println!("Forward to: {:?}", connection);
                            connection.send(data).await.unwrap();
                            let data = connection.recv().await.unwrap().unwrap();
                            stream.send(data).await.expect("stream should be open");
                        }
                        Destination::Local => {
                            println!("Serve Local");
                            println!(
                                "checked connection: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                            let res_data = storage.get(&key).unwrap();
                            println!(
                                "fetched: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                            let res = net::encode_response(net::KVResponseType::Result(res_data));
                            println!(
                                "encoded: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                            stream.send(res).await.expect("stream should be open");
                            println!(
                                "sent: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                        }
                    },
                    net::KVRequestType::Put(key, value) => {
                        match connections.lock().await.get(&key) {
                            Destination::Remote(connection) => {
                                connection.send(data).await.unwrap();
                                let data = connection.recv().await.unwrap().unwrap();
                                stream.send(data).await.expect("stream should be open");
                            }
                            Destination::Local => {
                                storage.put(&key, &value).unwrap();
                                let res = net::encode_response(net::KVResponseType::Ok);
                                stream.send(res).await.expect("stream should be open");
                            }
                        }
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
            println!("New Connection");
            while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                println!("New Stream");
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

        let mdns_search = task::spawn(Self::mdns_loop(
            self.connections.clone(),
            local_socket_addr,
            mdns_receiver,
        ));

        while let Some(connection) = self.server.accept().await {
            // spawn a new task for the connection
            tokio::spawn(Self::handle_connection(
                connection,
                self.connections.clone(),
                storage.clone(),
            ));
        }

        mdns_search.await??;

        Ok(())
    }
}

enum Destination<'a> {
    Remote(&'a mut S2SConnection),
    Local,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let local_ip = match local_ip_address::local_ip()? {
        std::net::IpAddr::V4(ip) => ip,
        std::net::IpAddr::V6(_) => todo!(),
    };

    let core_ids = core_affinity::get_core_ids().unwrap();
    let handles = core_ids
        .into_iter()
        .map(|id| {
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
                        let id = Uuid::new_v4();
                        let mut vnode = VNode::new(id, local_ip).unwrap();

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
