use crate::{
    message_clients::MessageClients,
    protocol::{KVReq, KVRequest, KVRes, KVResponse},
    ServerError,
};
use bytes::Bytes;
use futures::StreamExt;
use quic_transport::{DataStream, Decode, Encode, MessageStream, RequestWithMetadata};
use s2n_quic::{connection, stream::BidirectionalStream, Server};
use std::{net::Ipv4Addr, sync::Arc};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
};
use tracing::instrument;
use uuid::Uuid;

use super::s2s_mdns::S2SMDNS;

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));
/// NOTE: this certificate is to be used for demonstration purposes only!
pub static KEY_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/key.pem"));

pub(crate) struct S2SServer {
    storage: db::Database,
    pub(crate) mdns: S2SMDNS,
    server: s2n_quic::Server,
    rx: mpsc::Receiver<(KVReq, oneshot::Sender<KVRes>)>,
}

impl S2SServer {
    pub(crate) fn new(
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        rx: mpsc::Receiver<(KVReq, oneshot::Sender<KVRes>)>,
        storage: db::Database,
        clients: Arc<Mutex<MessageClients>>,
    ) -> Result<Self, ServerError> {
        let server = Server::builder()
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

        log::debug!("Starting S2S Server on Port: {}", port);

        let mdns = S2SMDNS::new(node_id, core_id, clients.clone(), local_ip, port);

        Ok(Self {
            storage,
            server,
            mdns,
            rx,
        })
    }

    #[instrument]
    async fn handle_local(req: KVReq, storage: db::Database) -> Result<KVRes, ServerError> {
        match req {
            KVReq::Get { key } => {
                let res_data = storage.get(&key)?;
                let res = KVRes::Result { result: res_data };
                Ok(res)
            }
            KVReq::Put { key, value } => {
                storage.put(&key, &value)?;
                let res = KVRes::Ok;
                Ok(res)
            }
        }
    }

    #[instrument(skip(send_tx))]
    async fn handle_local_req(
        req: KVRequest,
        storage: db::Database,
        send_tx: mpsc::Sender<Bytes>,
    ) -> Result<(), ServerError> {
        let id = req.id().clone();
        let res = Self::handle_local(req.into(), storage).await?;
        let res = RequestWithMetadata::new(id, res);
        let res: KVResponse = res.into();
        let encoded = res.encode();
        send_tx.send(encoded).await.expect("stream should be open");
        Ok(())
    }

    async fn handle_stream(stream: BidirectionalStream, storage: db::Database) {
        let (receive_stream, mut send_stream) = stream.split();
        let data_stream = DataStream::new(receive_stream);
        let mut request_stream: MessageStream<KVRequest> = MessageStream::new(data_stream);

        let (send_tx, mut send_rx): (mpsc::Sender<Bytes>, mpsc::Receiver<Bytes>) =
            mpsc::channel(100);

        tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                send_stream.send(data).await?;
            }

            Ok::<(), ServerError>(())
        });

        while let Some(Ok((req, _data))) = request_stream.next().await {
            let storage = storage.clone();
            let send_tx = send_tx.clone();
            tokio::spawn(async move { Self::handle_local_req(req.into(), storage, send_tx).await });
        }
    }

    async fn handle_connection(mut connection: connection::Connection, storage: db::Database) {
        while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
            tokio::spawn(Self::handle_stream(stream, storage.clone()));
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        let storage = self.storage.clone();

        loop {
            let storage = storage.clone();
            select! {
                Some(connection) = self.server.accept() => {
                    // spawn a new task for the connection
                    tokio::spawn(Self::handle_connection(
                        connection,
                        storage.clone(),
                    ));
                }
                Some((req, tx)) = self.rx.recv() => {
                    tokio::spawn(async move {
                        let res = Self::handle_local(req, storage).await?;
                        tx.send(res.clone()).expect("channel should be open");

                        Ok::<(), ServerError>(())
                    });
                }
            }
        }
    }
}
