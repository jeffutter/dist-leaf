use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use s2n_quic::Server;
use std::{error::Error, process, time::Duration};
use tokio::{task, time};

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));
/// NOTE: this certificate is to be used for demonstration purposes only!
pub static KEY_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/key.pem"));

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mdns = ServiceDaemon::new().expect("Failed to create daemon");

    let service_type = "_mdns-quic-db._udp.local.";
    let receiver = mdns.browse(service_type).expect("Failed to browse");

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
                    }
                    other_event => {
                        println!("Received other event: {:?}", &other_event);
                    }
                }
            }
        }
    });

    let mut server = Server::builder()
        .with_tls((CERT_PEM, KEY_PEM))?
        .with_io("0.0.0.0:0")?
        .start()?;

    let socket_addr = server.local_addr()?;

    let instance_name = format!("my_instance-{}", process::id());
    let host_ipv4 = socket_addr.ip().to_string();
    let host_name = format!("{}.local.", host_ipv4);
    let port = socket_addr.port();
    let my_service = ServiceInfo::new(
        service_type,
        &instance_name,
        &host_name,
        host_ipv4,
        port,
        None,
    )
    .unwrap()
    .enable_addr_auto();

    mdns.register(my_service)
        .expect("Failed to register our service");

    while let Some(mut connection) = server.accept().await {
        // spawn a new task for the connection
        tokio::spawn(async move {
            while let Ok(Some(mut stream)) = connection.accept_bidirectional_stream().await {
                // spawn a new task for the stream
                tokio::spawn(async move {
                    // echo any data back to the stream
                    while let Ok(Some(data)) = stream.receive().await {
                        stream.send(data).await.expect("stream should be open");
                    }
                });
            }
        });
    }

    mdns_search.await?;

    Ok(())
}
