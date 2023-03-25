mod mdns;
mod s2s_connection;
mod vnode;

use bytes::Bytes;
use env_logger::Env;
use s2s_connection::S2SConnections;
use std::{collections::HashMap, error::Error, thread};
use thiserror::Error;
use tokio::{
    runtime,
    sync::{mpsc, oneshot},
};
use uuid::Uuid;
use vnode::VNodeId;

use crate::vnode::VNode;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("decoding error")]
    Decoding(#[from] net::KVServerError),
    #[error("connection error")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("stream error")]
    Stream(#[from] s2n_quic::stream::Error),
    #[error("unknown server error")]
    Unknown,
    #[error("initialization error: {}", .0)]
    Initialization(String),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let local_ip = match local_ip_address::local_ip()? {
        std::net::IpAddr::V4(ip) => ip,
        std::net::IpAddr::V6(_) => todo!(),
    };

    let node_id = Uuid::new_v4();

    let core_ids = core_affinity::get_core_ids().unwrap();

    let (mut core_to_vnode_id, mut core_to_rx, core_to_tx): (
        HashMap<usize, VNodeId>,
        HashMap<usize, mpsc::Receiver<(Bytes, oneshot::Sender<Bytes>)>>,
        HashMap<VNodeId, mpsc::Sender<(Bytes, oneshot::Sender<Bytes>)>>,
    ) = core_ids.iter().fold(
        (HashMap::new(), HashMap::new(), HashMap::new()),
        |(mut core_to_vnode_id, mut rx_acc, mut tx_acc), id| {
            let id = id.id;
            let core_id = Uuid::new_v4();
            let vnode_id = VNodeId::new(node_id, core_id);
            let (tx, rx) = tokio::sync::mpsc::channel::<(Bytes, oneshot::Sender<Bytes>)>(1);
            core_to_vnode_id.insert(id, vnode_id.clone());
            rx_acc.insert(id, rx);
            tx_acc.insert(vnode_id, tx);
            (core_to_vnode_id, rx_acc, tx_acc)
        },
    );

    let handles = core_ids
        .into_iter()
        .map(|id| {
            let rx = core_to_rx.remove(&id.id).unwrap();
            let vnode_id = core_to_vnode_id.remove(&id.id).unwrap();
            let VNodeId { node_id, core_id } = vnode_id;
            let core_to_tx = core_to_tx.clone();
            thread::spawn(move || {
                // Pin this thread to a single CPU core.
                let res = core_affinity::set_for_current(id);
                if res {
                    let rt = runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    log::info!("Starting Thread: #{:?}", id);
                    rt.block_on(async {
                        let mut vnode = VNode::new(node_id, core_id, local_ip, rx, core_to_tx)?;

                        let _ = vnode.mdns.spawn();
                        vnode.run().await?;

                        Ok::<(), ServerError>(())
                    })
                    .unwrap();
                }

                Ok::<(), ServerError>(())
            })
        })
        .collect::<Vec<_>>();

    for handle in handles.into_iter() {
        handle.join().unwrap().unwrap();
    }

    Ok(())
}
