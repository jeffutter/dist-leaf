use crate::{
    protocol::{KVReq, KVRequest, KVRes, KVResponse},
    s2s_connection::S2SConnections,
    vnode::{client_mdns::ClientMDNS, Destination},
    ServerError,
};
use bytes::Bytes;
use futures::{Future, StreamExt};
use itertools::Itertools;
use quic_client::protocol::{KVRequestType, KVResponseType};
use quic_transport::{
    DataStream, Decode, Encode, MessageClient, MessageStream, RequestWithMetadata,
};
use s2n_quic::{connection, stream::BidirectionalStream, Server};
use std::{net::Ipv4Addr, sync::Arc, usize};
use tokio::{
    sync::{mpsc, Mutex},
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

pub(crate) struct ClientServer {
    connections: Arc<Mutex<S2SConnections>>,
    storage: db::Database,
    server: s2n_quic::Server,
}

impl ClientServer {
    pub(crate) fn new(
        node_id: Uuid,
        core_id: Uuid,
        local_ip: Ipv4Addr,
        storage: db::Database,
        connections: Arc<Mutex<S2SConnections>>,
    ) -> Result<Self, ServerError> {
        let server = Server::builder()
            .with_tls((CERT_PEM, KEY_PEM))
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .with_io("0.0.0.0:0")
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .start()
            .map_err(|e| ServerError::Initialization(e.to_string()))?;

        let port = server
            .local_addr()
            .map_err(|e| ServerError::Initialization(e.to_string()))?
            .port();

        log::debug!("Starting Client Server on Port: {}", port);

        let _mdns = ClientMDNS::new(node_id, core_id, local_ip, port);

        Ok(Self {
            connections,
            storage,
            server,
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

    async fn handle_request(
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

            if results.len() >= CONSISTENCY_LEVEL {
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

    fn handle_stream(
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

        while let Some(connection) = self.server.accept().await {
            tokio::spawn(Self::handle_connection(
                connection,
                self.connections.clone(),
                storage.clone(),
            ));
        }

        Ok(())
    }
}
