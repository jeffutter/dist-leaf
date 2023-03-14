use env_logger::Env;
use net::KVRequestType;
use tokio::io::{self, AsyncBufReadExt};

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or(
        "debug,netlink_proto=info,libp2p_ping=info,libp2p_swarm=info,libp2p_tcp=info,libp2p_mdns=info,libp2p_dns=info,yamux=info,multistream_select=info",
    ))
    .init();
    let mut dist_kv_server = net::DistKVServer::new().unwrap();
    let dist_kv_client = dist_kv_server.client();

    let cli = tokio::spawn(async move {
        let mut stdin = io::BufReader::new(io::stdin()).lines();

        loop {
            let next_line = stdin
                .next_line()
                .await
                .unwrap()
                .expect("Stdin not to close");

            match handle_input_line(next_line) {
                None => (),
                Some(request) => {
                    let req = dist_kv_client.send(request);
                    let res = req.await.unwrap();
                    println!("Result: {:?}", res);
                }
            }
        }
    });

    let server = dist_kv_server.run();

    let (res, _err) = tokio::join!(server, cli);
    res.unwrap();
}

fn handle_input_line(line: String) -> Option<KVRequestType> {
    let mut args = line.split(' ');

    let next = args.next().map(|x| x.to_uppercase());

    match next.as_deref() {
        Some("GET") => {
            let key = {
                match args.next() {
                    Some(key) => key,
                    None => {
                        println!("Expected key");
                        return None;
                    }
                }
            };
            Some(net::KVRequestType::Get(key.to_string()))
        }
        Some("PUT") => {
            let key = {
                match args.next() {
                    Some(key) => key,
                    None => {
                        println!("Expected key");
                        return None;
                    }
                }
            };
            let value = {
                match args.next() {
                    Some(value) => value,
                    None => {
                        println!("Expected value");
                        return None;
                    }
                }
            };

            Some(KVRequestType::Put(key.to_string(), value.to_string()))
        }
        _ => {
            println!("expected GET, PUT");
            None
        }
    }
}
