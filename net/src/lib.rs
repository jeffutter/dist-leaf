use libp2p::futures::StreamExt;
use libp2p::swarm::{keep_alive, NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{identity, mdns, ping, PeerId};
use std::error::Error;
use void;

pub async fn main() -> Result<(), Box<dyn Error>> {
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("Local peer id: {local_peer_id:?}");

    let transport = libp2p::tokio_development_transport(local_key)?;

    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), local_peer_id)?;

    let mut swarm = Swarm::with_tokio_executor(
        transport,
        Behaviour {
            ping: ping::Behaviour::default(),
            keep_alive: keep_alive::Behaviour::default(),
            mdns,
        },
        local_peer_id,
    );

    // Tell the swarm to listen on all interfaces and a random, OS-assigned
    // port.
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => println!("Listening on {address:?}"),
            SwarmEvent::Behaviour(MyBehaviourEvent::Mdns(mdns::Event::Discovered(addrs))) => {
                for (id, addr) in addrs {
                    swarm.dial(addr)?;
                    println!("Dialed {id}");
                }
            }
            SwarmEvent::Behaviour(event) => println!("{event:?}"),
            _ => {}
        }
    }
}

/// Our network behaviour.
///
/// For illustrative purposes, this includes the [`KeepAlive`](behaviour::KeepAlive) behaviour so a continuous sequence of
/// pings can be observed.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "MyBehaviourEvent")]
struct Behaviour {
    keep_alive: keep_alive::Behaviour,
    ping: ping::Behaviour,
    mdns: mdns::tokio::Behaviour,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum MyBehaviourEvent {
    Mdns(mdns::Event),
    Ping(ping::Event),
    Void,
}

impl From<mdns::Event> for MyBehaviourEvent {
    fn from(event: mdns::Event) -> Self {
        MyBehaviourEvent::Mdns(event)
    }
}

impl From<ping::Event> for MyBehaviourEvent {
    fn from(event: ping::Event) -> Self {
        MyBehaviourEvent::Ping(event)
    }
}

impl From<void::Void> for MyBehaviourEvent {
    fn from(_: void::Void) -> Self {
        MyBehaviourEvent::Void
    }
}
