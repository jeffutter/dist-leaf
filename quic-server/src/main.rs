use bytes::Bytes;
use consistent_hash_ring::{Ring, RingBuilder};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use s2n_quic::{client::Connect, stream::BidirectionalStream, Client, Connection, Server};
use std::{
    collections::HashMap,
    error::Error,
    net::{Ipv4Addr, SocketAddr},
    process,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::RwLock,
    task,
    time::{self, Instant},
};

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
    async fn new(addr: SocketAddr, client: Client) -> Result<Self, Box<dyn Error>> {
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

    async fn add(&mut self, addr: SocketAddr) -> Result<(), Box<dyn Error>> {
        if let None = self.connections.get(&addr) {
            let s2s_connection = S2SConnection::new(addr, self.client.clone()).await?;
            self.connections.insert(addr, s2s_connection);
            self.ring.insert(addr);
        }

        Ok(())
    }

    fn get(&mut self, key: String) -> Destination {
        let socket_addr = self.ring.get(key);
        if socket_addr == &self.local_addr {
            Destination::Local
        } else {
            let connection = self.connections.get_mut(socket_addr).unwrap();
            Destination::Remote(connection)
        }
    }
}

enum Destination<'a> {
    Remote(&'a mut S2SConnection),
    Local,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mdns = ServiceDaemon::new().expect("Failed to create daemon");

    let service_type = "_mdns-quic-db._udp.local.";
    let receiver = mdns.browse(service_type).expect("Failed to browse");

    let mut server = Server::builder()
        .with_tls((CERT_PEM, KEY_PEM))?
        .with_io("0.0.0.0:0")?
        .start()?;

    let local_ip = match local_ip_address::local_ip()? {
        std::net::IpAddr::V4(ip) => ip,
        std::net::IpAddr::V6(_) => todo!(),
    };

    let instance_name = format!("my_instance-{}", process::id());
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

    let server_socket_addr: SocketAddr = format!(
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

    let connections = S2SConnections::new(client, server_socket_addr);
    let connections = Arc::new(RwLock::new(connections));

    let connections1 = connections.clone();
    let mdns_search = task::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;
            while let Ok(event) = receiver.recv_async().await {
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

                        if socket_addr != server_socket_addr {
                            connections1.write().await.add(socket_addr).await.unwrap();
                        }
                    }
                    other_event => {
                        println!("Received other event: {:?}", &other_event);
                    }
                }
            }
        }
    });

    while let Some(mut connection) = server.accept().await {
        let connections1 = connections.clone();
        // spawn a new task for the connection
        tokio::spawn(async move {
            println!("New Connection");
            while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                println!("New Stream");
                let connections2 = connections1.clone();
                // spawn a new task for the stream
                tokio::spawn(async move {
                    // echo any data back to the stream
                    while let Ok(Some(data)) = stream.receive().await {
                        println!("New data");
                        let start = Instant::now();
                        let req = net::decode_request(data.as_ref()).unwrap();
                        println!(
                            "decoded: {}µs",
                            Instant::now().duration_since(start).as_micros()
                        );

                        match req {
                            net::KVRequestType::Get(key) => {
                                match connections2.write().await.get(key) {
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
                                        let res =
                                            net::encode_response(net::KVResponseType::Result(
                                                Some("HIT - GET".to_string()),
                                            ));
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
                                }
                            }
                            net::KVRequestType::Put(key, value) => {
                                match connections2.write().await.get(key) {
                                    Destination::Remote(connection) => {
                                        connection.send(data).await.unwrap();
                                        let data = connection.recv().await.unwrap().unwrap();
                                        stream.send(data).await.expect("stream should be open");
                                    }
                                    Destination::Local => {
                                        let res =
                                            net::encode_response(net::KVResponseType::Result(
                                                Some(format!("HIT - PUT {value}").to_string()),
                                            ));
                                        stream.send(res).await.expect("stream should be open");
                                    }
                                }
                            }
                        }
                    }
                });
            }
        });
    }

    mdns_search.await?;

    Ok(())
}
