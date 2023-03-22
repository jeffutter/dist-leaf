use s2n_quic::{client::Connect, stream::BidirectionalStream, Client, Connection};
use std::{error::Error, net::SocketAddr, sync::Arc};
use tokio::{sync::Mutex, time::Instant};

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));

#[derive(Clone)]
pub struct DistKVClient {
    stream: Arc<Mutex<BidirectionalStream>>,
    // client: Client,
    // connection: Connection,
}

impl DistKVClient {
    pub async fn new(port: &str) -> Result<Self, Box<dyn Error>> {
        let client = Client::builder()
            .with_tls(CERT_PEM)?
            .with_io("0.0.0.0:0")?
            .start()?;

        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
        let connect = Connect::new(addr).with_server_name("localhost");
        let mut connection = client.connect(connect).await?;

        // ensure the connection doesn't time out with inactivity
        connection.keep_alive(true)?;

        // open a new stream and split the receiving and sending sides
        let stream = connection.open_bidirectional_stream().await?;
        // Ok(Self { stream })
        // Ok(Self { connection })
        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
        })
    }

    pub async fn request(
        &mut self,
        req: net::KVRequestType,
    ) -> Result<net::KVResponseType, Box<dyn Error>> {
        let start = Instant::now();
        let encoded = net::encode_request(req);
        println!("Encoded Data: {:?}", encoded);
        println!(
            "encoded: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        self.stream.lock().await.send(encoded).await.unwrap();
        println!(
            "sent: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        if let Some(data) = self.stream.lock().await.receive().await.unwrap() {
            println!(
                "received: {}µs",
                Instant::now().duration_since(start).as_micros()
            );
            let decoded = net::decode_response(data.as_ref()).unwrap();
            println!(
                "decoded: {}µs",
                Instant::now().duration_since(start).as_micros()
            );
            Ok(decoded)
        } else {
            Ok(net::KVResponseType::Error("No Response".to_string()))
        }
    }
}
