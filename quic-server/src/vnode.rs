use crate::{
    mdns,
    protocol::{KVReq, KVRequest, KVRes, KVResponse},
    s2s_connection::{self, S2SConnections},
    ServerError,
};
use bytes::Bytes;
use futures::{Future, StreamExt};
use quic_client::DistKVClient;
use quic_transport::{
    ChannelMessageClient, DataStream, Decode, Encode, MessageClient, MessageStream, RequestWithId,
};
use s2n_quic::{connection, stream::BidirectionalStream, Server};
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
};
use tracing::instrument;
use uuid::Uuid;

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));
/// NOTE: this certificate is to be used for demonstration purposes only!
pub static KEY_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/key.pem"));

#[derive(Clone, Eq, Hash, PartialEq)]
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
    connections: Arc<Mutex<S2SConnections>>,
    storage: db::Database,
    pub(crate) mdns: mdns::MDNS,
    server: s2n_quic::Server,
    rx: mpsc::Receiver<(KVReq, oneshot::Sender<KVRes>)>,
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

        let server = Server::builder()
            .with_tls((CERT_PEM, KEY_PEM))
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        // node-id and core-id is too long
        let port = server
            .local_addr()
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .port();

        log::info!("Starting Server on Port: {}", port);

        let client = DistKVClient::new().unwrap();

        let mut connections = s2s_connection::S2SConnections::new(client, vnode_id);
        for (vnode_id, channel_message_client) in vnode_to_cmc {
            connections.add_channel(vnode_id, channel_message_client)
        }
        let connections = Arc::new(Mutex::new(connections));
        let storage = db::Database::new_tmp();

        let mdns = mdns::MDNS::new(node_id, core_id, connections.clone(), local_ip, port);

        Ok(Self {
            connections,
            storage,
            server,
            mdns,
            rx,
        })
    }

    #[instrument]
    async fn handle_local(req: KVReq, storage: db::Database) -> Result<KVRes, ServerError> {
        match req {
            KVReq::Get { key } => {
                let res_data = storage.get(&key)?;
                let res = KVRes::Result { result: res_data };
                Ok(res)
            }
            KVReq::Put { key, value } => {
                storage.put(&key, &value)?;
                let res = KVRes::Ok;
                Ok(res)
            }
        }
    }

    #[instrument(skip(connection, send_tx))]
    async fn forward_to_remote(
        req: KVRequest,
        connection: &mut Box<dyn MessageClient<KVReq, KVRes>>,
        send_tx: mpsc::Sender<Bytes>,
    ) -> Result<(), ServerError> {
        let id = req.id().clone();
        let res = connection.request(req.into()).await?;
        let res = RequestWithId::new(id, res);
        let res: KVResponse = res.into();
        let encoded = res.encode();
        send_tx.send(encoded).await.expect("stream should be open");
        Ok(())
    }

    #[instrument(skip(send_tx))]
    async fn handle_local_req(
        req: KVRequest,
        storage: db::Database,
        send_tx: mpsc::Sender<Bytes>,
    ) -> Result<(), ServerError> {
        let id = req.id().clone();
        let res = Self::handle_local(req.into(), storage).await?;
        let res = RequestWithId::new(id, res);
        let res: KVResponse = res.into();
        let encoded = res.encode();
        send_tx.send(encoded).await.expect("stream should be open");
        Ok(())
    }

    #[instrument(skip(connections, send_tx))]
    async fn handle_request(
        req: KVRequest,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
        send_tx: mpsc::Sender<Bytes>,
    ) -> Result<(), ServerError> {
        match connections.lock().await.get(req.key()) {
            Destination::Remote(connection) => {
                Self::forward_to_remote(req, connection, send_tx).await?;
            }
            Destination::Local => {
                Self::handle_local_req(req, storage, send_tx).await?;
            }
        }
        Ok::<(), ServerError>(())
    }

    fn handle_stream(
        stream: BidirectionalStream,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            let (receive_stream, mut send_stream) = stream.split();
            let data_stream = DataStream::new(receive_stream);
            let mut request_stream: MessageStream<KVRequest> = MessageStream::new(data_stream);

            let (send_tx, mut send_rx): (mpsc::Sender<Bytes>, mpsc::Receiver<Bytes>) =
                mpsc::channel(100);

            tokio::spawn(async move {
                while let Some(data) = send_rx.recv().await {
                    send_stream.send(data).await?;
                }

                Ok::<(), ServerError>(())
            });

            while let Some(Ok((req, _data))) = request_stream.next().await {
                let storage = storage.clone();
                let connections = connections.clone();
                let send_tx = send_tx.clone();
                tokio::spawn(async move {
                    Self::handle_request(req, connections, storage, send_tx).await
                });
            }
        }
    }

    fn handle_connection(
        mut connection: connection::Connection,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                tokio::spawn(Self::handle_stream(
                    stream,
                    connections.clone(),
                    storage.clone(),
                ));
            }
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), ServerError> {
        let storage = self.storage.clone();

        loop {
            let storage = storage.clone();
            select! {
                Some(connection) = self.server.accept() => {
                    // spawn a new task for the connection
                    tokio::spawn(Self::handle_connection(
                        connection,
                        self.connections.clone(),
                        storage.clone(),
                    ));
                }
                Some((req, tx)) = self.rx.recv() => {
                    tokio::spawn(async move {
                        let res = Self::handle_local(req, storage).await?;
                        tx.send(res.clone()).expect("channel should be open");

                        Ok::<(), ServerError>(())
                    });
                }
            }
        }
    }
}

pub(crate) enum Destination<'a> {
    Remote(&'a mut Box<dyn MessageClient<KVReq, KVRes>>),
    Local,
}
