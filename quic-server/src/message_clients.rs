use crate::{
    protocol::{ServerRequest, ServerResponse},
    vnode::LocalMessageClient,
    ServerError, VNodeId,
};
use consistent_hash_ring::{Ring, RingBuilder};
use net::{quic::Client, ChannelMessageClient, MessageClient};
use std::{collections::HashMap, fmt, net::SocketAddr};

pub(crate) struct MessageClients {
    clients: HashMap<VNodeId, Box<dyn MessageClient<ServerRequest, ServerResponse>>>,
    ring: Ring<VNodeId>,
    client: Client<ServerRequest, ServerResponse>,
}

impl fmt::Debug for MessageClients {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let vnodes = self.ring.vnodes();
        f.debug_struct("MessageClients")
            .field("clients", &self.clients)
            .field("vnodes", &vnodes)
            .finish()
    }
}

impl MessageClients {
    pub(crate) fn new(
        client: Client<ServerRequest, ServerResponse>,
        vnode_id: VNodeId,
        storage: db::Database,
    ) -> Self {
        let mut ring = RingBuilder::default().build();
        ring.insert(vnode_id.clone());

        let mut clients: HashMap<VNodeId, Box<dyn MessageClient<ServerRequest, ServerResponse>>> =
            HashMap::new();
        let local = Box::new(LocalMessageClient::new(storage));
        clients.insert(vnode_id.clone(), local);

        Self {
            clients,
            ring,
            client,
        }
    }

    pub(crate) async fn add_connection(
        &mut self,
        vnode_id: VNodeId,
        addr: SocketAddr,
    ) -> Result<(), ServerError> {
        if let None = self.clients.get(&vnode_id) {
            let connection = self.client.connect(addr).await.unwrap();
            let stream = connection.stream().await.unwrap();
            self.clients.insert(vnode_id.clone(), Box::new(stream));
            self.ring.insert(vnode_id);
        }

        Ok(())
    }

    pub(crate) fn add_channel(
        &mut self,
        vnode_id: VNodeId,
        channel_client: ChannelMessageClient<ServerRequest, ServerResponse>,
    ) {
        if let None = self.clients.get(&vnode_id) {
            self.clients
                .insert(vnode_id.clone(), Box::new(channel_client));
            self.ring.insert(vnode_id);
        }
    }

    pub(crate) fn get(
        &mut self,
        key: &str,
    ) -> Box<dyn MessageClient<ServerRequest, ServerResponse>> {
        let vnode_id = self.ring.get(key);
        self.clients.get(vnode_id).unwrap().box_clone()
    }

    pub(crate) fn get_for_vnodes(
        &mut self,
        vnode_ids: Vec<&VNodeId>,
    ) -> Vec<Box<dyn MessageClient<ServerRequest, ServerResponse>>> {
        vnode_ids
            .iter()
            .map(|vnode_id| self.clients.get(vnode_id).unwrap().box_clone())
            .collect()
    }

    pub(crate) async fn replicas(
        &mut self,
        key: &str,
        n: usize,
    ) -> Vec<(
        VNodeId,
        Box<dyn MessageClient<ServerRequest, ServerResponse>>,
    )> {
        self.ring
            .replicas(key)
            .take(n)
            .map(|vnode_id| {
                (
                    vnode_id.clone(),
                    self.clients.get(vnode_id).unwrap().box_clone(),
                )
            })
            .collect()
    }
}
