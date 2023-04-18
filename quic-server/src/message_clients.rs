use crate::{
    protocol::{KVReq, KVRequest, KVRes, KVResponse},
    vnode::LocalMessageClient,
    ServerError, VNodeId,
};
use consistent_hash_ring::{Ring, RingBuilder};
use quic_client::DistKVClient;
use quic_transport::{ChannelMessageClient, MessageClient};
use std::{collections::HashMap, net::SocketAddr};

pub(crate) struct MessageClients {
    clients: HashMap<VNodeId, Box<dyn MessageClient<KVReq, KVRes>>>,
    ring: Ring<VNodeId>,
    client: DistKVClient<KVReq, KVRequest, KVRes, KVResponse>,
}

impl MessageClients {
    pub(crate) fn new(
        client: DistKVClient<KVReq, KVRequest, KVRes, KVResponse>,
        vnode_id: VNodeId,
        storage: db::Database,
    ) -> Self {
        let mut ring = RingBuilder::default().build();
        ring.insert(vnode_id.clone());

        let mut clients: HashMap<VNodeId, Box<dyn MessageClient<KVReq, KVRes>>> = HashMap::new();
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
        channel_client: ChannelMessageClient<KVReq, KVRes>,
    ) {
        if let None = self.clients.get(&vnode_id) {
            self.clients
                .insert(vnode_id.clone(), Box::new(channel_client));
            self.ring.insert(vnode_id);
        }
    }

    pub(crate) fn get(&mut self, key: &str) -> Box<dyn MessageClient<KVReq, KVRes>> {
        let vnode_id = self.ring.get(key);
        self.clients.get(vnode_id).unwrap().box_clone()
    }

    pub(crate) async fn replicas(
        &mut self,
        key: &str,
        n: usize,
    ) -> Vec<Box<dyn MessageClient<KVReq, KVRes>>> {
        self.ring
            .replicas(key)
            .take(n)
            .map(|vnode_id| self.clients.get(vnode_id).unwrap().box_clone())
            .collect()
    }
}