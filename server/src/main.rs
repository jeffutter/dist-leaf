mod node_registry;
mod protocol;
mod vnode;

use clap::Parser;
use db::Database;
use env_logger::Env;
use net::quic::ServerError;
use std::{collections::HashMap, error::Error, thread};
use tokio::runtime;
use uuid::Uuid;
use vnode::VNodeId;

use crate::node_registry::ClientRegistry;
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
                &db::DBValue::new(&node_id.to_string(), hlc.new_timestamp()),
            )?;
            node_id
        }
        Ok(Some(data)) => data.data.parse()?,
        _ => unimplemented!(),
    };

    log::info!("Starting Server With ID: {:?}", node_id);

    let core_ids = core_affinity::get_core_ids().unwrap();

    let mut core_to_vnode_id: HashMap<usize, VNodeId> =
        core_ids
            .iter()
            .fold(HashMap::new(), |mut core_to_vnode_id, id| {
                let id = id.id;
                let id_key = format!("core_id:{}:{}", node_id, id);
                let core_id = match shared_db.get(&id_key) {
                    Ok(None) => {
                        let core_id = Uuid::new_v4();
                        shared_db
                            .put(
                                &id_key,
                                &db::DBValue::new(&core_id.to_string(), hlc.new_timestamp()),
                            )
                            .unwrap();
                        core_id
                    }
                    Ok(Some(data)) => data.data.parse().unwrap(),
                    _ => unimplemented!(),
                };
                let vnode_id = VNodeId::new(node_id, core_id);
                core_to_vnode_id.insert(id, vnode_id.clone());
                core_to_vnode_id
            });

    let client_registry = ClientRegistry::new();

    let handles = core_ids
        .into_iter()
        .map(|id| {
            let vnode_id = core_to_vnode_id.remove(&id.id).unwrap();
            let VNodeId { node_id, core_id } = vnode_id;
            let client_registry = client_registry.clone();

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
                            VNode::new(node_id, core_id, local_ip, client_registry, data_path)
                                .await?;

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