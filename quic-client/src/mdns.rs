use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

use mdns_sd::{Receiver, ServiceDaemon, ServiceEvent};
use tokio::{
    sync::{watch, Mutex},
    task, time,
};

pub struct MDNS {
    receiver: Receiver<ServiceEvent>,
    connections: Arc<Mutex<HashSet<SocketAddr>>>,
    tx: Arc<Mutex<watch::Sender<HashSet<SocketAddr>>>>,
    pub rx: watch::Receiver<HashSet<SocketAddr>>,
}

impl MDNS {
    pub(crate) fn new() -> Self {
        let mdns = ServiceDaemon::new().expect("Failed to create daemon");
        let service_type = "_mdns-quic-db._udp.local.";
        let receiver = mdns.browse(service_type).expect("Failed to browse");
        let connections = Arc::new(Mutex::new(HashSet::new()));

        let (tx, rx) = watch::channel(HashSet::new());

        Self {
            receiver,
            connections,
            tx: Arc::new(Mutex::new(tx)),
            rx,
        }
    }

    pub(crate) async fn spawn(&self) -> task::JoinHandle<()> {
        let mut interval = time::interval(Duration::from_secs(30));
        let receiver = self.receiver.clone();
        let cons = self.connections.clone();
        let tx = self.tx.clone();

        task::spawn(async move {
            loop {
                interval.tick().await;
                while let Ok(event) = receiver.recv_async().await {
                    match event {
                        ServiceEvent::ServiceResolved(info) => {
                            log::debug!(
                                "Resolved a new service: {} {}",
                                info.get_fullname(),
                                info.get_port()
                            );

                            let socket_addr: SocketAddr = SocketAddr::new(
                                info.get_addresses()
                                    .iter()
                                    .next()
                                    .unwrap()
                                    .to_owned()
                                    .into(),
                                info.get_port(),
                            );

                            let mut cons = cons.lock().await;
                            if cons.insert(socket_addr) {
                                tx.lock().await.send(cons.clone()).unwrap();
                            };
                        }
                        ServiceEvent::SearchStarted(_) => (),
                        other_event => {
                            log::debug!("Received other event: {:?}", &other_event);
                        }
                    }
                }
            }
        })
    }
}
