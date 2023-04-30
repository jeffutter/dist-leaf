mod message_clients;
mod protocol;
mod vnode;

use clap::Parser;
use db::Database;
use env_logger::Env;
use quic_transport::{quic::ServerError, ChannelMessageClient};
use std::{collections::HashMap, error::Error, thread};
use tokio::{
    runtime,
    sync::{mpsc, oneshot},
};
use uuid::Uuid;
use vnode::VNodeId;

use crate::protocol::{ServerRequest, ServerResponse};
use crate::vnode::VNode;
use tempfile::TempDir;

pub mod server_capnp {
    include!(concat!(env!("OUT_DIR"), "/server_capnp.rs"));
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    data_path: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let subscriber = tracing_subscriber::FmtSubscriber::new();
    // tracing::subscriber::set_global_default(subscriber)?;
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let base_path = match args.data_path {
        None => {
            let p = TempDir::new().unwrap();
            p.path().to_path_buf()
        }
        Some(path) => path,
    };

    let local_ip = match local_ip_address::local_ip()? {
        std::net::IpAddr::V4(ip) => ip,
        std::net::IpAddr::V6(_) => todo!(),
    };

    let hlc = uhlc::HLC::default();

    let mut shared_db_path = base_path.clone();
    shared_db_path.push("shared");
    let shared_db = Database::new(&shared_db_path);
    let node_id = match shared_db.get("node_id") {
        Ok(None) => {
            let node_id = Uuid::new_v4();
            shared_db.put(
                "node_id",
                &db::DBValue {
                    ts: hlc.new_timestamp(),
                    data: node_id.to_string().into(),
                },
            )?;
            node_id
        }
        Ok(Some(data)) => data.data.parse()?,
        _ => unimplemented!(),
    };

    log::info!("Starting Server With ID: {:?}", node_id);

    let core_ids = core_affinity::get_core_ids().unwrap();

    let (mut core_to_vnode_id, mut core_to_rx, core_to_cmc): (
        HashMap<usize, VNodeId>,
        HashMap<usize, mpsc::Receiver<(ServerRequest, oneshot::Sender<ServerResponse>)>>,
        HashMap<VNodeId, ChannelMessageClient<ServerRequest, ServerResponse>>,
    ) = core_ids.iter().fold(
        (HashMap::new(), HashMap::new(), HashMap::new()),
        |(mut core_to_vnode_id, mut rx_acc, mut tx_acc), id| {
            let id = id.id;
            let id_key = format!("core_id:{}:{}", node_id, id);
            let core_id = match shared_db.get(&id_key) {
                Ok(None) => {
                    let core_id = Uuid::new_v4();
                    shared_db
                        .put(
                            &id_key,
                            &db::DBValue {
                                ts: hlc.new_timestamp(),
                                data: core_id.to_string().into(),
                            },
                        )
                        .unwrap();
                    core_id
                }
                Ok(Some(data)) => data.data.parse().unwrap(),
                _ => unimplemented!(),
            };
            let vnode_id = VNodeId::new(node_id, core_id);
            let (tx, rx) = mpsc::channel::<(ServerRequest, oneshot::Sender<ServerResponse>)>(1);
            let channel_message_client = ChannelMessageClient::new(tx);
            core_to_vnode_id.insert(id, vnode_id.clone());
            rx_acc.insert(id, rx);
            tx_acc.insert(vnode_id, channel_message_client);
            (core_to_vnode_id, rx_acc, tx_acc)
        },
    );

    let handles = core_ids
        .into_iter()
        .map(|id| {
            let rx = core_to_rx.remove(&id.id).unwrap();
            let vnode_id = core_to_vnode_id.remove(&id.id).unwrap();
            let VNodeId { node_id, core_id } = vnode_id;
            let core_to_cmc = core_to_cmc.clone();

            let mut data_path = base_path.clone();
            // data_path.push(core_id.to_string());
            data_path.push(id.id.to_string());

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
                        let mut vnode =
                            VNode::new(node_id, core_id, local_ip, rx, core_to_cmc, data_path)?;

                        let _ = vnode.s2s_server.mdns.spawn().await;
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
