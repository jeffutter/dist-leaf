pub(crate) mod client_mdns;
mod client_server;
pub(crate) mod s2s_mdns;
mod s2s_server;

use crate::{
    node_registry::{ClientRegistry, ConnectionRegistry},
    protocol::{ServerRequest, ServerResponse},
    vnode::{client_server::ClientServer, s2s_server::S2SServer},
};
use async_trait::async_trait;
use db::DBValue;
use net::{quic::ServerError, MessageClient, TransportError};
use std::fmt::Debug;
use std::net::Ipv4Addr;
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
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        client_registry: ClientRegistry<ServerRequest, ServerResponse>,
        data_path: std::path::PathBuf,
    ) -> Result<Self, ServerError> {
        let vnode_id = VNodeId::new(node_id, core_id);
        let storage = db::Database::new(&data_path);

        let local_message_client = Box::new(LocalMessageClient::new(storage.clone()));
        let connection_registry =
            ConnectionRegistry::new(client_registry.clone(), vnode_id, local_message_client);
        let s2s_server =
            S2SServer::new(node_id, core_id, local_ip, storage.clone(), client_registry).await?;
        let client_server = ClientServer::new(node_id, core_id, local_ip, connection_registry)?;

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
