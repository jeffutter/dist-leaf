use crate::{
    client_mdns,
    protocol::{KVReq, KVRequest, KVRes, KVResponse},
    s2s_connection::{self, S2SConnections},
    s2s_mdns, ServerError,
};
use bytes::Bytes;
use futures::{Future, StreamExt};
use itertools::Itertools;
use quic_client::{
    protocol::{KVRequestType, KVResponseType},
    DistKVClient,
};
use quic_transport::{
    ChannelMessageClient, DataStream, Decode, Encode, MessageClient, MessageStream,
    RequestWithMetadata,
};
use s2n_quic::{connection, stream::BidirectionalStream, Server};
use std::{collections::HashMap, net::Ipv4Addr, sync::Arc, usize};
use tokio::{
    select,
    sync::{mpsc, oneshot, Mutex},
    task::JoinSet,
};
use tracing::{event, instrument, Level};
use uuid::Uuid;

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));
/// NOTE: this certificate is to be used for demonstration purposes only!
pub static KEY_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/key.pem"));

static REPLICATION_FACTOR: usize = 3;
static CONSISTENCY_LEVEL: usize = match (REPLICATION_FACTOR / 2, REPLICATION_FACTOR % 2) {
    (n, 0) => n,
    (n, _) => n + 1,
};

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
    connections: Arc<Mutex<S2SConnections>>,
    storage: db::Database,
    pub(crate) s2s_mdns: s2s_mdns::S2SMDNS,
    s2s_server: s2n_quic::Server,
    client_server: s2n_quic::Server,
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

        let s2s_server = Server::builder()
            .with_tls((CERT_PEM, KEY_PEM))
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        let client_server = Server::builder()
            .with_tls((CERT_PEM, KEY_PEM))
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        let client_port = client_server
            .local_addr()
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .port();

        let s2s_port = s2s_server
            .local_addr()
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .port();

        log::debug!("Starting Client Server on Port: {}", client_port);
        log::debug!("Starting S2S Server on Port: {}", s2s_port);

        let client = DistKVClient::new().unwrap();

        let mut connections = s2s_connection::S2SConnections::new(client, vnode_id);
        for (vnode_id, channel_message_client) in vnode_to_cmc {
            connections.add_channel(vnode_id, channel_message_client)
        }
        let connections = Arc::new(Mutex::new(connections));
        let storage = db::Database::new_tmp();

        let s2s_mdns =
            s2s_mdns::S2SMDNS::new(node_id, core_id, connections.clone(), local_ip, s2s_port);

        let _client_mdns = client_mdns::ClientMDNS::new(node_id, core_id, local_ip, client_port);

        Ok(Self {
            connections,
            storage,
            client_server,
            s2s_server,
            s2s_mdns,
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
        mut connection: Box<dyn MessageClient<KVReq, KVRes>>,
        send_tx: mpsc::Sender<Bytes>,
    ) -> Result<(), ServerError> {
        let id = req.id().clone();
        let res = connection.request(req.into()).await?;
        let res = RequestWithMetadata::new(id, res);
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
        let res = RequestWithMetadata::new(id, res);
        let res: KVResponse = res.into();
        let encoded = res.encode();
        send_tx.send(encoded).await.expect("stream should be open");
        Ok(())
    }

    async fn handle_client_request(
        req: KVRequestType,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
        send_tx: mpsc::Sender<Bytes>,
    ) -> Result<(), ServerError> {
        let mut join_set: JoinSet<Result<KVResponseType, ServerError>> = JoinSet::new();
        let mut cs = connections.lock().await;
        let replicas = cs.replicas(req.key(), REPLICATION_FACTOR).await;

        event!(Level::DEBUG, "replicas" = ?replicas);

        let local = replicas.iter().filter(|destination| match destination {
            Destination::Local => true,
            _ => false,
        });

        let remote = replicas
            .iter()
            .filter_map(|destination| match destination {
                Destination::Remote(conn) => Some(conn.box_clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        if local.count() > 0 {
            let req = req.clone();
            let storage = storage.clone();
            join_set.spawn({
                async move {
                    let id = req.id().clone();
                    let res = Self::handle_local(req.into(), storage).await?;
                    let res: KVResponseType = match res {
                        KVRes::Error { error } => KVResponseType::Error { id, error },
                        KVRes::Result { result } => KVResponseType::Result { id, result },
                        KVRes::Ok => KVResponseType::Ok(id),
                    };
                    Ok::<KVResponseType, ServerError>(res)
                }
            });
        }

        for connection in remote {
            let req = req.clone();

            join_set.spawn({
                async move {
                    let id = req.id().clone();
                    let res = connection.box_clone().request(req.into()).await?;
                    let res: KVResponseType = match res {
                        KVRes::Error { error } => KVResponseType::Error { id, error },
                        KVRes::Result { result } => KVResponseType::Result { id, result },
                        KVRes::Ok => KVResponseType::Ok(id),
                    };

                    Ok::<KVResponseType, ServerError>(res)
                }
            });
        }

        let mut results: Mutex<Vec<Option<Result<KVResponseType, ServerError>>>> =
            Mutex::new(Vec::new());

        while let Some(res) = join_set.join_next().await {
            let results = results.get_mut();

            match res {
                Ok(Ok(res)) => results.push(Some(Ok(res))),
                Ok(Err(e)) => results.push(Some(Err(e))),
                Err(_e) => results.push(Some(Err(ServerError::Unknown))),
            }

            let target_reqs = match req {
                KVRequestType::Get { .. } => CONSISTENCY_LEVEL,
                KVRequestType::Put { .. } => REPLICATION_FACTOR,
            };

            if results.len() >= target_reqs {
                let unique_res: Vec<KVResponseType> = results
                    .iter()
                    .filter_map(|x| match x {
                        Some(Ok(res)) => Some(res.clone()),
                        _ => None,
                    })
                    .unique()
                    .collect();

                if unique_res.len() == 1 {
                    let res = &unique_res[0];

                    send_tx
                        .send(res.encode())
                        .await
                        .expect("channel should be open");

                    event!(
                        Level::INFO,
                        "Results matched with {} request(s)",
                        results.len()
                    );

                    break;
                } else if results.len() == REPLICATION_FACTOR {
                    let res = KVResponse::Error {
                        id: *req.id(),
                        error: "Results did not match".to_string(),
                    };
                    send_tx
                        .send(res.encode())
                        .await
                        .expect("channel should be open");

                    event!(Level::ERROR, results = ?results, "Results did not match");
                }
            }
        }

        // For put requests, we want to let all of the writes finish, even if we've hit the
        // requested Consistency Level
        if let KVRequestType::Put { .. } = req {
            join_set.detach_all();
        }

        Ok::<(), ServerError>(())
    }

    fn handle_s2s_stream(
        stream: BidirectionalStream,
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
                let send_tx = send_tx.clone();
                tokio::spawn(
                    async move { Self::handle_local_req(req.into(), storage, send_tx).await },
                );
            }
        }
    }

    fn handle_client_stream(
        stream: BidirectionalStream,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            let (receive_stream, mut send_stream) = stream.split();
            let data_stream = DataStream::new(receive_stream);
            let mut request_stream: MessageStream<KVRequestType> = MessageStream::new(data_stream);

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
                    Self::handle_client_request(req, connections, storage, send_tx).await
                });
            }
        }
    }

    fn handle_s2s_connection(
        mut connection: connection::Connection,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                tokio::spawn(Self::handle_s2s_stream(stream, storage.clone()));
            }
        }
    }

    fn handle_client_connection(
        mut connection: connection::Connection,
        connections: Arc<Mutex<S2SConnections>>,
        storage: db::Database,
    ) -> impl Future<Output = ()> {
        async move {
            while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                tokio::spawn(Self::handle_client_stream(
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
                Some(connection) = self.client_server.accept() => {
                    // spawn a new task for the connection
                    tokio::spawn(Self::handle_client_connection(
                        connection,
                        self.connections.clone(),
                        storage.clone(),
                    ));
                }
                Some(connection) = self.s2s_server.accept() => {
                    // spawn a new task for the connection
                    tokio::spawn(Self::handle_s2s_connection(
                        connection,
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

#[derive(Clone, Debug)]
pub(crate) enum Destination<'a> {
    Remote(&'a Box<dyn MessageClient<KVReq, KVRes>>),
    Local,
}
