use bytes::Bytes;
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use rand::seq::IteratorRandom;
use s2n_quic::{client::Connect, stream::BidirectionalStream, Client, Connection, Server};
use std::{
    collections::HashMap,
    error::Error,
    net::{Ipv4Addr, SocketAddr},
    process,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::RwLock, task, time};

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

    let connections: HashMap<SocketAddr, S2SConnection> = HashMap::new();
    let connections = Arc::new(RwLock::new(connections));
    let client = Client::builder()
        .with_tls(CERT_PEM)?
        .with_io("0.0.0.0:0")?
        .start()?;

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
                            let client = client.clone();
                            let mut connections = connections1.write().await;
                            if let None = connections.get(&socket_addr) {
                                let connection =
                                    S2SConnection::new(socket_addr, client).await.unwrap();
                                connections.insert(socket_addr, connection);
                            }
                        }
                    }
                    other_event => {
                        println!("Received other event: {:?}", &other_event);
                    }
                }
            }
        }
    });

    let connections1 = connections.clone();
    while let Some(mut connection) = server.accept().await {
        let connections2 = connections1.clone();
        // spawn a new task for the connection
        tokio::spawn(async move {
            let connections3 = connections2.clone();
            while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                let connections4 = connections3.clone();
                // spawn a new task for the stream
                tokio::spawn(async move {
                    // echo any data back to the stream
                    while let Ok(Some(data)) = stream.receive().await {
                        let mut connections = connections4.write().await;

                        let connection = connections.values_mut().choose(&mut rand::thread_rng());

                        match connection {
                            Some(c) => {
                                println!("Echoing: {:?}", data);
                                // stream.send(data).await.expect("stream should be open");
                                c.send(data.into()).await.unwrap();
                                let data = c.recv().await.unwrap().unwrap();
                                stream.send(data).await.expect("stream should be open");
                                // println!("Received: {:?}", data);
                            }
                            None => {
                                println!("No Connections");
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
