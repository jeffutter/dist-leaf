use crate::{
    message_clients::MessageClients,
    protocol::{ServerRequest, ServerResponse},
    ServerError,
};
use bytes::Bytes;
use db::DBValue;
use futures::StreamExt;
use local_sync::mpsc;
use monoio::select;
use quic_transport::{DataStream, Encode, MessageStream};
use s2n_quic::{connection, stream::BidirectionalStream, Server};
use std::{
    net::Ipv4Addr,
    sync::{Arc, Mutex},
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
    rx: Arc<
        Mutex<std::sync::mpsc::Receiver<(ServerRequest, std::sync::mpsc::Sender<ServerResponse>)>>,
    >,
}

impl S2SServer {
    pub(crate) fn new(
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        rx: std::sync::mpsc::Receiver<(ServerRequest, std::sync::mpsc::Sender<ServerResponse>)>,
        storage: db::Database,
        clients: Arc<futures::lock::Mutex<MessageClients>>,
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
            rx: Arc::new(Mutex::new(rx)),
        })
    }

    #[instrument]
    async fn handle_local(
        req: ServerRequest,
        storage: db::Database,
    ) -> Result<ServerResponse, ServerError> {
        match req {
            ServerRequest::Get { request_id, key } => {
                let result = storage.get(&key)?;
                let res = match result {
                    Some(data) => ServerResponse::Result {
                        request_id,
                        data_id: Some(data.ts),
                        result: Some(data.data.to_string()),
                    },
                    None => ServerResponse::Result {
                        request_id,
                        data_id: None,
                        result: None,
                    },
                };
                Ok(res)
            }
            ServerRequest::Put {
                request_id,
                key,
                value,
            } => {
                storage.put(&key, &DBValue::new(&value, request_id))?;
                let res = ServerResponse::Ok { request_id };
                Ok(res)
            }
        }
    }

    async fn handle_local_req(
        req: ServerRequest,
        storage: db::Database,
        send_tx: mpsc::bounded::Tx<Bytes>,
    ) -> Result<(), ServerError> {
        let res = Self::handle_local(req, storage).await?;
        let encoded = res.encode();
        send_tx.send(encoded).await.expect("stream should be open");
        Ok(())
    }

    async fn handle_stream(stream: BidirectionalStream, storage: db::Database) {
        let (receive_stream, mut send_stream) = stream.split();
        let data_stream = DataStream::new(receive_stream);
        let mut request_stream: MessageStream<ServerRequest> = MessageStream::new(data_stream);

        let (send_tx, mut send_rx): (mpsc::bounded::Tx<Bytes>, mpsc::bounded::Rx<Bytes>) =
            mpsc::bounded::channel(1024);

        monoio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                send_stream.send(data).await?;
            }

            Ok::<(), ServerError>(())
        });

        while let Some(Ok((req, _data))) = request_stream.next().await {
            let storage = storage.clone();
            let send_tx = send_tx.clone();
            monoio::spawn(
                async move { Self::handle_local_req(req.into(), storage, send_tx).await },
            );
        }
    }

    async fn handle_connection(mut connection: connection::Connection, storage: db::Database) {
        while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
            monoio::spawn(Self::handle_stream(stream, storage.clone()));
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        let storage = self.storage.clone();
        let storage1 = self.storage.clone();
        let rx = self.rx.clone();

        monoio::spawn(async move {
            loop {
                let storage = storage.clone();
                if let Ok((req, tx)) = rx.lock().unwrap().recv() {
                    let tx = tx.clone();
                    // Intentionally don't check for errors/unrwrap as `tx` may have been
                    // closed by the other end if the request has already been filled
                    #[allow(unused_must_use)]
                    monoio::spawn(async move {
                        let res = Self::handle_local(req, storage).await?;
                        tx.send(res);

                        Ok::<(), ServerError>(())
                    });
                }
            }
        });

        loop {
            let storage = storage1.clone();

            select! {
                // Quic
                Some(connection) = self.server.accept() => {
                    // spawn a new task for the connection
                    monoio::spawn(Self::handle_connection(
                        connection,
                        storage,
                    ));
                }
                // Channel
                // Some((req, tx)) = self.rx.recv() => {
                //     // Intentionally don't check for errors/unrwrap as `tx` may have been
                //     // closed by the other end if the request has already been filled
                //     #[allow(unused_must_use)]
                //     monoio::spawn(async move {
                //         let res = Self::handle_local(req, storage).await?;
                //         tx.send(res);
                //
                //         Ok::<(), ServerError>(())
                //     });
                // }
            }
        }
    }
}
