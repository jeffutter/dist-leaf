use crate::{vnode::Destination, ServerError, VNodeId};
use bytes::Bytes;
use consistent_hash_ring::{Ring, RingBuilder};
use s2n_quic::{client::Connect, stream::BidirectionalStream, Client};
use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub(crate) struct S2SConnection {
    stream: BidirectionalStream,
}

impl S2SConnection {
    async fn new(addr: SocketAddr, client: Client) -> Result<Self, ServerError> {
        let connect = Connect::new(addr).with_server_name("localhost");
        let mut connection = client.connect(connect).await?;
        connection.keep_alive(true)?;
        let stream = connection.open_bidirectional_stream().await?;

        Ok(Self { stream })
    }

    pub(crate) async fn send(&mut self, data: Bytes) -> Result<(), ServerError> {
        self.stream.send(data).await?;
        Ok(())
    }

    pub(crate) async fn recv(&mut self) -> Result<Option<Bytes>, ServerError> {
        let data = self.stream.receive().await?;
        Ok(data)
    }
}

pub(crate) struct S2SConnections {
    connections: HashMap<VNodeId, S2SConnection>,
    channels: HashMap<VNodeId, mpsc::Sender<(Bytes, oneshot::Sender<Bytes>)>>,
    ring: Ring<VNodeId>,
    client: Client,
    vnode_id: VNodeId,
}

impl S2SConnections {
    pub(crate) fn new(client: Client, vnode_id: VNodeId) -> Self {
        let mut ring = RingBuilder::default().build();
        ring.insert(vnode_id.clone());

        Self {
            connections: HashMap::new(),
            channels: HashMap::new(),
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
            let s2s_connection = S2SConnection::new(addr, self.client.clone()).await?;
            self.connections.insert(vnode_id.clone(), s2s_connection);
            self.ring.insert(vnode_id);
        }

        Ok(())
    }

    pub(crate) fn add_channel(
        &mut self,
        vnode_id: VNodeId,
        channel: mpsc::Sender<(Bytes, oneshot::Sender<Bytes>)>,
    ) {
        if let None = self.channels.get(&vnode_id) {
            self.channels.insert(vnode_id.clone(), channel);
            self.ring.insert(vnode_id);
        }
    }

    pub(crate) fn get(&mut self, key: &str) -> Destination {
        let vnode_id = self.ring.get(key);

        match vnode_id {
            vnode_id if vnode_id == &self.vnode_id => Destination::Local,
            VNodeId {
                node_id,
                core_id: _,
            } if node_id == &self.vnode_id.node_id => {
                let channel = self.channels.get_mut(vnode_id).unwrap();
                Destination::Adjacent(channel)
            }
            vnode_id => {
                let connection = self.connections.get_mut(vnode_id).unwrap();
                Destination::Remote(connection)
            }
        }
    }
}
