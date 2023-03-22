use net::{decode_response, encode_request, KVRequestType, KVResponseType, ProtoReader};
use s2n_quic::{client::Connect, stream::BidirectionalStream, Client};
use std::{error::Error, net::SocketAddr, sync::Arc};
use tokio::{sync::Mutex, time::Instant};

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));

#[derive(Clone)]
pub struct DistKVClient {
    stream: Arc<Mutex<BidirectionalStream>>,
    proto_reader: Arc<Mutex<ProtoReader>>,
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
        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
            proto_reader: Arc::new(Mutex::new(ProtoReader::new())),
        })
    }

    pub async fn request(&mut self, req: KVRequestType) -> Result<KVResponseType, Box<dyn Error>> {
        let start = Instant::now();
        let encoded = encode_request(req);
        log::debug!("Encoded Data: {:?}", encoded);
        log::debug!(
            "encoded: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        self.stream.lock().await.send(encoded).await.unwrap();
        log::debug!(
            "sent: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        let mut stream = self.stream.lock().await;
        let mut proto_reader = self.proto_reader.lock().await;

        loop {
            match stream.receive().await.unwrap() {
                Some(data) => {
                    proto_reader.add_data(data);
                    match proto_reader.read_message() {
                        Some(data) => {
                            log::debug!(
                                "received: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                            let decoded = decode_response(data.as_ref()).unwrap();
                            log::debug!(
                                "decoded: {}µs",
                                Instant::now().duration_since(start).as_micros()
                            );
                            return Ok(decoded);
                        }
                        None => {
                            continue;
                        }
                    }
                }
                None => {
                    return Ok(KVResponseType::Error("No Response".to_string()));
                }
            }
        }
    }
}
