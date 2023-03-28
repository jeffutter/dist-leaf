use futures::StreamExt;
use net::{encode_request, KVRequestType, KVResponseType};
use quic_transport::ResponseStream;
use s2n_quic::{client::Connect, stream::SendStream, Client, Connection};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc},
};
use thiserror::Error;
use tokio::{
    sync::{oneshot, Mutex},
    time::Instant,
};

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("initialization error: {}", .0)]
    Initialization(String),
    #[error("unknown client error")]
    Unknown,
}

pub struct DistKVClient {
    client: Client,
}

impl DistKVClient {
    pub fn new() -> Result<Self, ClientError> {
        let client = Client::builder()
            .with_tls(CERT_PEM)
            .map_err(|e| ClientError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ClientError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ClientError::Initialization(e.to_string()))?;

        Ok(Self { client })
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<DistKVConnection, ClientError> {
        let connect = Connect::new(addr).with_server_name("localhost");
        let mut connection = self.client.connect(connect).await?;
        // ensure the connection doesn't time out with inactivity
        connection.keep_alive(true)?;

        let conn = DistKVConnection::new(connection).await;

        Ok(conn)
    }
}

#[derive(Debug)]
pub struct DistKVConnection {
    connection: Arc<Mutex<Connection>>,
}

impl DistKVConnection {
    pub async fn new(connection: Connection) -> Self {
        Self {
            connection: Arc::new(Mutex::new(connection)),
        }
    }

    pub async fn stream(&self) -> Result<DistKVStream, ClientError> {
        DistKVStream::new(self.connection.clone()).await
    }
}

pub struct DistKVStream {
    pending_requests: Arc<Mutex<HashMap<u64, oneshot::Sender<KVResponseType>>>>,
    request_counter: AtomicU64,
    send_stream: SendStream,
}

impl DistKVStream {
    async fn new(connection: Arc<Mutex<Connection>>) -> Result<Self, ClientError> {
        // open a new stream and split the receiving and sending sides
        let stream = connection.lock().await.open_bidirectional_stream().await?;
        let (receive_stream, send_stream) = stream.split();

        let pending_requests: Arc<Mutex<HashMap<u64, oneshot::Sender<KVResponseType>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let pending_requests1 = pending_requests.clone();
        tokio::spawn(async move {
            let mut response_stream: ResponseStream = receive_stream.into();
            while let Some(Ok((req, _data))) = response_stream.next().await {
                let tx = pending_requests1.lock().await.remove(req.id()).unwrap();
                tx.send(req).unwrap();
            }
        });

        Ok(Self {
            send_stream,
            pending_requests,

            request_counter: AtomicU64::new(0),
        })
    }

    pub async fn request(&mut self, req: KVRequest) -> Result<KVResponseType, ClientError> {
        let start = Instant::now();
        let id = self
            .request_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let encoded = encode_request(KVRequestWithId::new(id, req).into());
        log::debug!("Encoded Data: {:?}", encoded);
        log::debug!(
            "encoded: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        self.send_stream.send(encoded).await.unwrap();
        let (tx, rx) = oneshot::channel();
        self.pending_requests.lock().await.insert(id, tx);
        log::debug!(
            "sent: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        let result = rx.await.expect("channel should be open");
        log::debug!(
            "received: {}µs",
            Instant::now().duration_since(start).as_micros()
        );
        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KVRequest {
    Get { key: String },
    Put { key: String, value: String },
}

struct KVRequestWithId {
    id: u64,
    kv_request: KVRequest,
}

impl KVRequestWithId {
    fn new(id: u64, kv_request: KVRequest) -> Self {
        Self { id, kv_request }
    }
}

impl Into<KVRequestType> for KVRequestWithId {
    fn into(self) -> KVRequestType {
        match self.kv_request {
            KVRequest::Get { key } => KVRequestType::Get { id: self.id, key },
            KVRequest::Put { key, value } => KVRequestType::Put {
                id: self.id,
                key,
                value,
            },
        }
    }
}
