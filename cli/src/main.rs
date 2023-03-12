use env_logger::Env;
use tokio::io::{self, AsyncBufReadExt};

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or(
        "debug,netlink_proto=info,libp2p_ping=info,libp2p_swarm=info,libp2p_tcp=info,libp2p_mdns=info,libp2p_dns=info,yamux=info,multistream_select=info",
    ))
    .init();
    let (command_channel, network) = net::start();

    let cli = tokio::spawn(async move {
        let mut stdin = io::BufReader::new(io::stdin()).lines();

        loop {
            let next_line = stdin
                .next_line()
                .await
                .unwrap()
                .expect("Stdin not to close");

            if let Err(_) = command_channel.send(next_line).await {
                println!("receiver dropped");
                return;
            }
        }
    });

    let (res, _err) = tokio::join!(network, cli);
    res.unwrap();
}
