pub(crate) mod client_mdns;
mod client_server;
pub(crate) mod s2s_mdns;
mod s2s_server;

use crate::{
    protocol::{ServerRequest, ServerResponse},
    server_context::ServerContext,
    vnode::{client_server::ClientServer, s2s_server::S2SServer},
};
use async_trait::async_trait;
use db::DBValue;
use net::{MessageClient, ServerError, TransportError};
use std::fmt::Debug;
use tokio::try_join;
use tracing::instrument;
use uuid::Uuid;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(crate) struct VNodeId {
    pub(crate) node_id: Uuid,
    pub(crate) core_id: Uuid,
}

impl VNodeId {
    pub(crate) fn new(node_id: Uuid, core_id: Uuid) -> Self {
        Self { node_id, core_id }
    }
}

pub(crate) struct VNode {
    pub(crate) s2s_server: S2SServer,
    client_server: ClientServer,
}

impl VNode {
    pub(crate) async fn new(
        context: ServerContext<ServerRequest, ServerResponse>,
    ) -> Result<Self, ServerError> {
        let storage = db::Database::new(&context.data_path);
        let s2s_server = S2SServer::new(context.clone(), storage.clone()).await?;
        let client_server = ClientServer::new(context.clone(), storage).await?;

        Ok(Self {
            client_server,
            s2s_server,
        })
    }

    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        try_join!(self.s2s_server.run(), self.client_server.run())?;

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct LocalMessageClient {
    storage: db::Database,
}

impl LocalMessageClient {
    pub(crate) fn new(storage: db::Database) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl MessageClient<ServerRequest, ServerResponse> for LocalMessageClient {
    #[instrument(skip(self), fields(message_client = "Local"))]
    async fn request(&mut self, req: ServerRequest) -> Result<ServerResponse, TransportError> {
        match req {
            ServerRequest::Get { request_id, key } => {
                let res_data = self
                    .storage
                    .get(&key)
                    .map_err(|e| TransportError::UnknownMsg(e.to_string()))?;

                let res = match res_data {
                    Some(data) => ServerResponse::Result {
                        request_id,
                        data_id: Some(data.ts),
                        digest: Some(data.digest),
                        result: Some(data.data.to_string()),
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
            ServerRequest::Digest { request_id, key } => {
                let res_data = self
                    .storage
                    .get(&key)
                    .map_err(|e| TransportError::UnknownMsg(e.to_string()))?;

                let res = match res_data {
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
                self.storage
                    .put(&key, &DBValue::new(&value, request_id))
                    .map_err(|e| TransportError::UnknownMsg(e.to_string()))?;
                let res = ServerResponse::Ok { request_id };
                Ok(res)
            }
            ServerRequest::Delete { request_id, key } => {
                // TODO: might need to check that request_id is older than saved
                self.storage
                    .delete(&key, request_id)
                    .map_err(|e| TransportError::UnknownMsg(e.to_string()))?;
                let res = ServerResponse::Ok { request_id };
                Ok(res)
            }
        }
    }

    fn box_clone(&self) -> Box<dyn MessageClient<ServerRequest, ServerResponse>> {
        Box::new(LocalMessageClient {
            storage: self.storage.clone(),
        })
    }
}
