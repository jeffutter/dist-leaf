pub(crate) mod client_mdns;
mod client_server;
pub(crate) mod s2s_mdns;
mod s2s_server;

use crate::{
    protocol::{KVReq, KVRes},
    message_clients,
    vnode::{client_server::ClientServer, s2s_server::S2SServer},
    ServerError,
};
use async_trait::async_trait;
use quic_client::DistKVClient;
use quic_transport::{ChannelMessageClient, MessageClient, TransportError};
use std::fmt::Debug;
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    try_join,
};
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
    pub(crate) fn new(
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        rx: mpsc::Receiver<(KVReq, oneshot::Sender<KVRes>)>,
        vnode_to_cmc: HashMap<VNodeId, ChannelMessageClient<KVReq, KVRes>>,
    ) -> Result<Self, ServerError> {
        let vnode_id = VNodeId::new(node_id, core_id);
        let client = DistKVClient::new().unwrap();
        let storage = db::Database::new_tmp();
        let mut connections =
            message_clients::MessageClients::new(client, vnode_id, storage.clone());
        for (vnode_id, channel_message_client) in vnode_to_cmc {
            connections.add_channel(vnode_id, channel_message_client)
        }
        let connections = Arc::new(Mutex::new(connections));

        let s2s_server = S2SServer::new(
            node_id,
            core_id,
            local_ip,
            rx,
            storage.clone(),
            connections.clone(),
        )?;
        let client_server = ClientServer::new(
            node_id,
            core_id,
            local_ip,
            storage.clone(),
            connections.clone(),
        )?;

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
impl MessageClient<KVReq, KVRes> for LocalMessageClient {
    #[instrument(skip(self), fields(message_client = "Local"))]
    async fn request(&mut self, req: KVReq) -> Result<KVRes, TransportError> {
        match req {
            KVReq::Get { key } => {
                let res_data = self
                    .storage
                    .get(&key)
                    .map_err(|e| TransportError::UnknownMsg(e.to_string()))?;
                let res = KVRes::Result { result: res_data };
                Ok(res)
            }
            KVReq::Put { key, value } => {
                self.storage
                    .put(&key, &value)
                    .map_err(|e| TransportError::UnknownMsg(e.to_string()))?;
                let res = KVRes::Ok;
                Ok(res)
            }
        }
    }

    fn box_clone(&self) -> Box<dyn MessageClient<KVReq, KVRes>> {
        Box::new(LocalMessageClient {
            storage: self.storage.clone(),
        })
    }
}
