mod node_registry;
mod protocol;
mod server_context;
mod vnode;

use clap::Parser;
use db::Database;
use env_logger::Env;
use net::ServerError;
use std::{error::Error, thread};
use tokio::runtime;
use uuid::Uuid;
use vnode::VNodeId;

use crate::node_registry::ClientRegistry;
use crate::protocol::{ServerRequest, ServerResponse};
use crate::server_context::ServerContext;
use crate::vnode::VNode;
use tempfile::TempDir;

pub mod server_capnp {
    include!(concat!(env!("OUT_DIR"), "/server_capnp.rs"));
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, value_enum)]
    transport: server_context::Transport,
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

    let client_registry = ClientRegistry::new();

    let handles = core_ids
        .into_iter()
        .map(|id| {
            let id_key = format!("core_id:{}:{}", node_id, id.id);
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

            let mut data_path = base_path.clone();
            data_path.push(id.id.to_string());

            let context: ServerContext<ServerRequest, ServerResponse> = ServerContext::new(
                node_id,
                core_id,
                args.transport,
                local_ip,
                data_path.clone(),
                client_registry.clone(),
            );

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
                        let mut vnode = VNode::new(context).await?;

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
