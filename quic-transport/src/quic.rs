use async_trait::async_trait;
use futures::StreamExt;
use s2n_quic::{
    client::Connect, connection::Handle, stream::BidirectionalStream, Client as QClient,
    Connection as QConnection, Server as QServer,
};
use std::{
    fmt::Debug,
    marker::{PhantomData, Send},
    net::SocketAddr,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::{
    ClientError, DataStream, Decode, Encode, MessageClient as TMessageClient, MessageStream,
    TransportError,
};

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));
/// NOTE: this certificate is to be used for demonstration purposes only!
pub static KEY_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/key.pem"));

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("transport error")]
    Decoding(#[from] TransportError),
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("stream error")]
    Stream(#[from] s2n_quic::stream::Error),
    #[error("unknown server error")]
    Unknown,
    #[error("initialization error: {}", .0)]
    Initialization(String),
    #[error("response error: {}", .0)]
    Response(String),
}

pub struct Client<Req, Res> {
    client: QClient,
    req: PhantomData<Req>,
    res: PhantomData<Res>,
}

impl<Req, Res> Client<Req, Res>
where
    Req: Encode + Decode + Debug + Send,
    Res: Encode + Decode + Debug + Sync + Send + 'static,
{
    pub fn new() -> Result<Self, ClientError> {
        let client = QClient::builder()
            .with_tls(CERT_PEM)
            .map_err(|e| ClientError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ClientError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ClientError::Initialization(e.to_string()))?;

        Ok(Self {
            client,
            req: PhantomData,
            res: PhantomData,
        })
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<Connection<Req, Res>, ClientError> {
        let connect = Connect::new(addr).with_server_name("localhost");
        let mut connection = self.client.connect(connect).await?;
        // ensure the connection doesn't time out with inactivity
        connection.keep_alive(true)?;

        let conn = Connection::new(connection).await;

        Ok(conn)
    }
}

#[derive(Clone)]
pub struct Connection<Req, Res> {
    connection: Handle,
    req: PhantomData<Req>,
    res: PhantomData<Res>,
}

impl<Req, Res> Connection<Req, Res>
where
    Req: Encode + Decode + Debug + Send,
    Res: Encode + Decode + Debug + Sync + Send + 'static,
{
    pub async fn new(connection: QConnection) -> Self {
        Self {
            connection: connection.handle(),
            req: PhantomData,
            res: PhantomData,
        }
    }

    pub async fn stream(&self) -> Result<MessageClient<Req, Res>, ClientError> {
        MessageClient::new(self.connection.clone())
            .await
            .map_err(|e| e.into())
    }
}

pub struct MessageClient<Req, Res> {
    handle: Handle,
    phantom1: PhantomData<Req>,
    phantom2: PhantomData<Res>,
}

impl<Req, Res> MessageClient<Req, Res>
where
    Req: Encode + Decode + Debug,
    Res: Encode + Decode + Debug + Sync + Send + 'static,
{
    pub async fn new(connection: Handle) -> Result<Self, TransportError> {
        Ok(Self {
            handle: connection,
            phantom1: PhantomData,
            phantom2: PhantomData,
        })
    }
}

#[async_trait]
impl<Req, Res> TMessageClient<Req, Res> for MessageClient<Req, Res>
where
    Req: Encode + Decode + Send + Sync + Debug + 'static,
    Res: Encode + Decode + Send + Sync + Debug + 'static,
{
    #[instrument(skip(self), fields(message_client = "Quic"))]
    async fn request(&mut self, req: Req) -> Result<Res, TransportError> {
        let stream = self.handle.open_bidirectional_stream().await?;
        let (receive_stream, mut send_stream) = stream.split();

        let encoded = req.encode();
        send_stream.send(encoded).await.unwrap();
        let data_stream = DataStream::new(receive_stream);
        let mut response_stream = MessageStream::<Res, s2n_quic::stream::Error>::new(data_stream);
        let (result, _data) = response_stream
            .next()
            .await
            .expect("stream should be open")?;

        Ok(result.into())
    }

    fn box_clone(&self) -> Box<dyn TMessageClient<Req, Res>> {
        Box::new(MessageClient {
            handle: self.handle.clone(),
            phantom1: PhantomData,
            phantom2: PhantomData,
        })
    }
}

impl<Req, Res> Debug for MessageClient<Req, Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageClient")
            .field("socket", &self.handle.remote_addr().unwrap())
            .finish()
    }
}

#[async_trait]
pub trait Handler<Req, Res>: Send + 'static {
    async fn call(&mut self, req: Req, send_tx: mpsc::Sender<Res>) -> Result<(), ServerError>;
    fn box_clone(&self) -> Box<dyn Handler<Req, Res>>;
}

pub struct Server<Req, Res> {
    handler: Box<dyn Handler<Req, Res>>,
    server: s2n_quic::Server,
    pub port: u16,
    phantom_data: PhantomData<Res>,
}

impl<Req, Res> Server<Req, Res>
where
    Req: Encode + Decode + Send + Sync + Debug + 'static,
    Res: Encode + Decode + Send + Sync + Debug + 'static,
{
    pub fn new(handler: impl Handler<Req, Res>) -> Result<Self, ServerError> {
        let server = QServer::builder()
            .with_tls((CERT_PEM, KEY_PEM))
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        let port = server
            .local_addr()
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .port();

        Ok(Self {
            server,
            port,
            handler: Box::new(handler),
            phantom_data: PhantomData,
        })
    }

    pub async fn run(&mut self) -> Result<(), ServerError> {
        while let Some(connection) = self.server.accept().await {
            let handler = self.handler.box_clone();
            tokio::spawn(Self::handle_connection(connection, handler));
        }

        Ok(())
    }

    async fn handle_connection(mut connection: QConnection, handler: Box<dyn Handler<Req, Res>>) {
        while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
            let handler = handler.box_clone();
            tokio::spawn(Self::handle_stream(stream, handler));
        }
    }

    async fn handle_stream(stream: BidirectionalStream, handler: Box<dyn Handler<Req, Res>>) {
        let (receive_stream, mut send_stream) = stream.split();
        let data_stream = DataStream::new(receive_stream);
        let mut request_stream: MessageStream<Req, s2n_quic::stream::Error> =
            MessageStream::new(data_stream);

        let (send_tx, mut send_rx): (mpsc::Sender<Res>, mpsc::Receiver<Res>) = mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                let encoded = data.encode();
                send_stream.send(encoded).await?;
            }

            Ok::<(), ServerError>(())
        });

        while let Some(Ok((req, _data))) = request_stream.next().await {
            let send_tx = send_tx.clone();
            let mut handler = handler.box_clone();
            tokio::spawn(async move { handler.call(req, send_tx).await });
        }
    }
}
