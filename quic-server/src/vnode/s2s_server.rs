use crate::{
    message_clients::MessageClients,
    protocol::{ServerRequest, ServerResponse},
};
use async_trait::async_trait;
use db::DBValue;
use quic_transport::quic::{Handler, Server, ServerError};
use std::{net::Ipv4Addr, sync::Arc};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
};
use tracing::instrument;
use uuid::Uuid;

use super::s2s_mdns::S2SMDNS;

struct ServerHandler {
    storage: db::Database,
}

impl ServerHandler {
    fn new(storage: db::Database) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl Handler<ServerRequest, ServerResponse> for ServerHandler {
    #[instrument(skip(self))]
    async fn call(
        &mut self,
        req: ServerRequest,
        send_tx: mpsc::Sender<ServerResponse>,
    ) -> Result<(), ServerError> {
        let res = S2SServer::handle_local(req, self.storage.clone()).await?;
        send_tx.send(res).await.expect("stream should be open");
        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Handler<ServerRequest, ServerResponse>> {
        Box::new(Self::new(self.storage.clone()))
    }
}

pub(crate) struct S2SServer {
    storage: db::Database,
    pub(crate) mdns: S2SMDNS,
    server: Server<ServerRequest, ServerResponse>,
    rx: mpsc::Receiver<(ServerRequest, oneshot::Sender<ServerResponse>)>,
}

impl S2SServer {
    pub(crate) fn new(
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        rx: mpsc::Receiver<(ServerRequest, oneshot::Sender<ServerResponse>)>,
        storage: db::Database,
        clients: Arc<Mutex<MessageClients>>,
    ) -> Result<Self, ServerError> {
        let handler = ServerHandler::new(storage.clone());
        let server = Server::new(handler)?;

        log::debug!("Starting S2S Server on Port: {}", server.port);

        let mdns = S2SMDNS::new(node_id, core_id, clients.clone(), local_ip, server.port);

        Ok(Self {
            storage,
            server,
            mdns,
            rx,
        })
    }

    #[instrument]
    async fn handle_local(
        req: ServerRequest,
        storage: db::Database,
    ) -> Result<ServerResponse, ServerError> {
        match req {
            ServerRequest::Get { request_id, key } => {
                // TODO: fix this error type
                let result = storage.get(&key).map_err(|_| ServerError::Unknown)?;
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
                // TODO: fix this error type
                storage
                    .put(&key, &DBValue::new(&value, request_id))
                    .map_err(|_| ServerError::Unknown)?;
                let res = ServerResponse::Ok { request_id };
                Ok(res)
            }
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        loop {
            let storage = self.storage.clone();

            select! {
                // Server
                Ok(()) = self.server.run() => { },
                // Channel
                Some((req, tx)) = self.rx.recv() => {
                    // Intentionally don't check for errors/unrwrap as `tx` may have been
                    // closed by the other end if the request has already been filled
                    #[allow(unused_must_use)]
                    tokio::spawn(async move {
                        let res = Self::handle_local(req, storage).await?;
                        tx.send(res);

                        Ok::<(), ServerError>(())
                    });
                }
            }
        }
    }
}
