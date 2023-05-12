use std::{collections::HashMap, sync::Arc};

use consistent_hash_ring::{Ring, RingBuilder};
use net::{Client, Connection, MessageClient, TransportError};
use tokio::{runtime::Handle, sync::RwLock};
use tracing::{event, instrument, Level};

use crate::vnode::VNodeId;

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone)]
pub(crate) enum Locality {
    Channel = 0,
    Remote = 1,
}

#[derive(Clone)]
pub(crate) struct ClientRegistry<Req, Res>
where
    Req: Clone,
    Res: Clone,
{
    state: Arc<RwLock<HashMap<VNodeId, (Locality, Box<dyn Client<Req, Res> + Sync + Send>)>>>,
    ring: Arc<RwLock<Ring<VNodeId>>>,
}

impl<Req, Res> ClientRegistry<Req, Res>
where
    Req: Clone,
    Res: Clone,
{
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
            ring: Arc::new(RwLock::new(RingBuilder::default().build())),
        }
    }

    #[instrument(skip(self, client))]
    pub async fn add(
        &mut self,
        vnode_id: VNodeId,
        locality: &Locality,
        client: impl Client<Req, Res> + Sync + Send + 'static,
    ) {
        let mut state = self.state.write().await;
        match state.get(&vnode_id) {
            Some((stored_locality, _stored_client)) => {
                if locality <= stored_locality {
                    event!(
                        Level::INFO,
                        "Inserting {:?} {:?} overwriting {:?}",
                        vnode_id,
                        locality,
                        stored_locality
                    );
                    state.insert(vnode_id.clone(), (locality.clone(), Box::new(client)));
                    self.ring.write().await.insert(vnode_id);
                } else {
                    event!(
                        Level::INFO,
                        "Skipping {:?} {:?} found {:?}",
                        vnode_id,
                        locality,
                        stored_locality
                    );
                }
            }
            None => {
                event!(Level::INFO, "Inserting {:?} {:?}", vnode_id, locality,);
                state.insert(vnode_id.clone(), (locality.clone(), Box::new(client)));
                self.ring.write().await.insert(vnode_id);
            }
        }
    }

    async fn get(&self, vnode_id: &VNodeId) -> Option<Box<dyn Client<Req, Res>>> {
        let state = self.state.read().await;

        match state.get(vnode_id) {
            Some((_, client)) => Some(client.box_clone()),
            None => None,
        }
    }

    #[instrument(skip(self))]
    async fn replicas(&self, key: &str, n: usize) -> Vec<VNodeId> {
        self.ring
            .read()
            .await
            .replicas(key)
            .take(n)
            .cloned()
            .collect()
    }
}

impl<Req, Res> std::fmt::Debug for ClientRegistry<Req, Res>
where
    Req: Clone,
    Res: Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let handle = Handle::current();
        let _ = handle.enter();
        let state = self.state.clone();
        let state = futures::executor::block_on(async {
            state
                .read()
                .await
                .iter()
                .map(|(vnode_id, (locality, _))| (vnode_id.clone(), locality.clone()))
                .collect::<Vec<_>>()
        });

        f.debug_struct("ClientRegistry")
            // .field("state", &self.state)
            .field("state", &state)
            // .field("ring", &self.ring)
            .finish()
    }
}

pub(crate) struct ConnectionRegistry<Req, Res>
where
    Req: Clone,
    Res: Clone,
{
    vnode_id: VNodeId,
    message_client: Box<dyn MessageClient<Req, Res>>,
    state: Arc<RwLock<HashMap<VNodeId, Box<dyn Connection<Req, Res>>>>>,
    client_registry: ClientRegistry<Req, Res>,
}

impl<Req, Res> ConnectionRegistry<Req, Res>
where
    Req: Clone,
    Res: Clone,
{
    pub fn new(
        client_registry: ClientRegistry<Req, Res>,
        vnode_id: VNodeId,
        message_client: Box<dyn MessageClient<Req, Res>>,
    ) -> Self {
        Self {
            vnode_id,
            message_client,
            state: Arc::new(RwLock::new(HashMap::new())),
            client_registry,
        }
    }

    #[instrument(skip(self))]
    pub async fn stream(
        &mut self,
        vnode_id: &VNodeId,
    ) -> Result<Option<Box<dyn MessageClient<Req, Res>>>, TransportError> {
        if vnode_id == &self.vnode_id {
            Ok(Some(self.message_client.box_clone()))
        } else {
            let mut state = self.state.write().await;

            match state.get(&vnode_id) {
                Some(connection) => Ok(Some(connection.stream().await?)),
                None => match self.client_registry.get(&vnode_id).await {
                    Some(client) => {
                        let connection = client.connection().await?;
                        state.insert(vnode_id.clone(), connection.box_clone());
                        Ok(Some(connection.stream().await?))
                    }
                    None => Ok(None),
                },
            }
        }
    }

    #[instrument(skip(self))]
    pub(crate) async fn replicas(
        &mut self,
        key: &str,
        n: usize,
    ) -> Vec<(VNodeId, Box<dyn MessageClient<Req, Res>>)> {
        let mut res: Vec<(VNodeId, Box<dyn MessageClient<Req, Res>>)> = Vec::new();

        for vnode_id in self.client_registry.replicas(key, n).await {
            let stream = self.stream(&vnode_id).await.unwrap().unwrap();
            res.push((vnode_id.clone(), stream));
        }

        res
    }

    pub(crate) fn box_clone(&self) -> Self {
        Self {
            vnode_id: self.vnode_id.clone(),
            message_client: self.message_client.box_clone(),
            state: self.state.clone(),
            client_registry: self.client_registry.clone(),
        }
    }
}

impl<Req, Res> std::fmt::Debug for ConnectionRegistry<Req, Res>
where
    Req: Clone,
    Res: Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionRegistry")
            .field("vnode_id", &self.vnode_id)
            .field("message_client", &self.message_client)
            // .field("state", &self.state)
            .field("client_registry", &self.client_registry)
            .finish()
    }
}
