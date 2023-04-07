pub(crate) mod client_mdns;
mod client_server;
pub(crate) mod s2s_mdns;
mod s2s_server;

use crate::{
    protocol::{KVReq, KVRes},
    s2s_connection,
    vnode::{client_server::ClientServer, s2s_server::S2SServer},
    ServerError,
};
use quic_client::DistKVClient;
use quic_transport::{ChannelMessageClient, MessageClient};
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    try_join,
};
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
        let mut connections = s2s_connection::S2SConnections::new(client, vnode_id);
        for (vnode_id, channel_message_client) in vnode_to_cmc {
            connections.add_channel(vnode_id, channel_message_client)
        }
        let connections = Arc::new(Mutex::new(connections));

        let storage = db::Database::new_tmp();
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

#[derive(Clone, Debug)]
pub(crate) enum Destination<'a> {
    Remote(&'a Box<dyn MessageClient<KVReq, KVRes>>),
    Local,
}
