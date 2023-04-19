pub mod protocol;

use quic_transport::{Decode, Encode, QuicMessageClient, TransportError};
use s2n_quic::connection::Handle;
use s2n_quic::{client::Connect, Client, Connection};
use std::fmt::Debug;
use std::{marker::PhantomData, net::SocketAddr};
use thiserror::Error;

pub mod client_capnp {
    include!(concat!(env!("OUT_DIR"), "/client_capnp.rs"));
}

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("initialization error: {}", .0)]
    Initialization(String),
    #[error("transport error")]
    Transport(#[from] TransportError),
    #[error("unknown client error")]
    Unknown,
}

pub struct DistKVClient<Req, Res> {
    client: Client,
    req: PhantomData<Req>,
    res: PhantomData<Res>,
}

impl<Req, Res> DistKVClient<Req, Res>
where
    Req: Encode + Decode + Debug + Send,
    Res: Encode + Decode + Debug + Sync + Send + 'static,
{
    pub fn new() -> Result<Self, ClientError> {
        let client = Client::builder()
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

    pub async fn connect(
        &self,
        addr: SocketAddr,
    ) -> Result<DistKVConnection<Req, Res>, ClientError> {
        let connect = Connect::new(addr).with_server_name("localhost");
        let mut connection = self.client.connect(connect).await?;
        // ensure the connection doesn't time out with inactivity
        connection.keep_alive(true)?;

        let conn = DistKVConnection::new(connection).await;

        Ok(conn)
    }
}

#[derive(Clone)]
pub struct DistKVConnection<Req, Res> {
    connection: Handle,
    req: PhantomData<Req>,
    res: PhantomData<Res>,
}

impl<Req, Res> DistKVConnection<Req, Res>
where
    Req: Encode + Decode + Debug + Send,
    Res: Encode + Decode + Debug + Sync + Send + 'static,
{
    pub async fn new(connection: Connection) -> Self {
        Self {
            connection: connection.handle(),
            req: PhantomData,
            res: PhantomData,
        }
    }

    pub async fn stream(&self) -> Result<QuicMessageClient<Req, Res>, ClientError> {
        QuicMessageClient::new(self.connection.clone())
            .await
            .map_err(|e| e.into())
    }
}
