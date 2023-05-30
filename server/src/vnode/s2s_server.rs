use crate::{
    node_registry::Locality,
    protocol::{ServerRequest, ServerResponse},
    server_context::{ServerContext, Transport},
};
use async_trait::async_trait;
use db::DBValue;
use futures::{
    channel::{mpsc, oneshot},
    select, FutureExt, SinkExt, StreamExt,
};
use net::{channel::ChannelClient, quic::QuicServer, tcp::TcpServer, Handler, Server, ServerError};
use tracing::instrument;

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
        mut send_tx: mpsc::Sender<ServerResponse>,
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
    server: Box<dyn Server<ServerRequest, ServerResponse>>,
    rx: mpsc::Receiver<(ServerRequest, oneshot::Sender<ServerResponse>)>,
}

impl S2SServer {
    pub(crate) async fn new(
        mut context: ServerContext<ServerRequest, ServerResponse>,
        storage: db::Database,
    ) -> Result<Self, ServerError> {
        let handler = ServerHandler::new(storage.clone());
        let server: Box<dyn Server<ServerRequest, ServerResponse>> = match context.transport {
            Transport::TCP => Box::new(TcpServer::new(handler).await?),
            Transport::Quic => Box::new(QuicServer::new(handler)?),
        };

        log::debug!("Starting S2S Server on Port: {}", server.port());

        let (tx, rx) = mpsc::channel::<(ServerRequest, oneshot::Sender<ServerResponse>)>(1);
        let client = ChannelClient::new(tx);
        context
            .client_registry
            .add(context.vnode_id.clone(), &Locality::Channel, client)
            .await;

        let mdns = S2SMDNS::new(context, server.port());

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
                        digest: Some(data.digest),
                        data_id: Some(data.ts),
                        result: Some(data.data.to_string()),
                    },
                    None => ServerResponse::Result {
                        request_id,
                        digest: None,
                        data_id: None,
                        result: None,
                    },
                };
                Ok(res)
            }
            ServerRequest::Digest { request_id, key } => {
                // TODO: fix this error type
                let result = storage.get(&key).map_err(|_| ServerError::Unknown)?;
                let res = match result {
                    Some(data) => ServerResponse::Result {
                        request_id,
                        data_id: Some(data.ts),
                        digest: Some(data.digest),
                        result: None,
                    },
                    None => ServerResponse::Result {
                        request_id,
                        data_id: None,
                        digest: None,
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
            ServerRequest::Delete { request_id, key } => {
                // TODO: fix this error type
                // TODO: might need to check that request_id is older than saved
                storage
                    .delete(&key, request_id)
                    .map_err(|_| ServerError::Unknown)?;
                let res = ServerResponse::Ok { request_id };
                Ok(res)
            }
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        loop {
            let storage = self.storage.clone();
            let mut run = Box::pin(self.server.run()).fuse();

            select! {
                // Server
                _ = run => {},
                // Channel
                x = self.rx.next() => {
                    match x {
                        Some((req, tx)) => {
                            // Intentionally don't check for errors/unrwrap as `tx` may have been
                            // closed by the other end if the request has already been filled
                            #[allow(unused_must_use)]
                            tokio::spawn(async move {
                                let res = Self::handle_local(req, storage).await?;
                                tx.send(res);

                                Ok::<(), ServerError>(())
                            });

                            },
                        None => { }
                    }
                },
            }
        }
    }
}
