use crate::{
    protocol::{KVReq, KVRequest, KVRes, KVResponse},
    vnode::Destination,
    ServerError, VNodeId,
};
use consistent_hash_ring::{Ring, RingBuilder};
use quic_client::DistKVClient;
use quic_transport::{ChannelMessageClient, MessageClient};
use std::{collections::HashMap, net::SocketAddr};

pub(crate) struct S2SConnections {
    connections: HashMap<VNodeId, Box<dyn MessageClient<KVReq, KVRes>>>,
    ring: Ring<VNodeId>,
    client: DistKVClient<KVReq, KVRequest, KVRes, KVResponse>,
    vnode_id: VNodeId,
}

impl S2SConnections {
    pub(crate) fn new(
        client: DistKVClient<KVReq, KVRequest, KVRes, KVResponse>,
        vnode_id: VNodeId,
    ) -> Self {
        let mut ring = RingBuilder::default().build();
        ring.insert(vnode_id.clone());

        Self {
            connections: HashMap::new(),
            ring,
            client,
            vnode_id,
        }
    }

    pub(crate) async fn add_connection(
        &mut self,
        vnode_id: VNodeId,
        addr: SocketAddr,
    ) -> Result<(), ServerError> {
        if let None = self.connections.get(&vnode_id) {
            let connection = self.client.connect(addr).await.unwrap();
            let stream = connection.stream().await.unwrap();
            self.connections.insert(vnode_id.clone(), Box::new(stream));
            self.ring.insert(vnode_id);
        }

        Ok(())
    }

    pub(crate) fn add_channel(
        &mut self,
        vnode_id: VNodeId,
        channel_client: ChannelMessageClient<KVReq, KVRes>,
    ) {
        if let None = self.connections.get(&vnode_id) {
            self.connections
                .insert(vnode_id.clone(), Box::new(channel_client));
            self.ring.insert(vnode_id);
        }
    }

    pub(crate) fn get(&mut self, key: &str) -> Destination {
        let vnode_id = self.ring.get(key);

        match vnode_id {
            vnode_id if vnode_id == &self.vnode_id => Destination::Local,
            vnode_id => {
                let client = self.connections.get_mut(vnode_id).unwrap();
                Destination::Remote(client)
            }
        }
    }
}
