use s2n_quic::{client::Connect, Client};
use std::{error::Error, net::SocketAddr, thread};
use tokio::sync::mpsc::channel;

/// NOTE: this certificate is to be used for demonstration purposes only!
pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = Client::builder()
        .with_tls(CERT_PEM)?
        .with_io("0.0.0.0:0")?
        .start()?;

    let args: Vec<String> = std::env::args().collect();
    let port = args.get(1).unwrap();

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    let connect = Connect::new(addr).with_server_name("localhost");
    let mut connection = client.connect(connect).await?;

    // ensure the connection doesn't time out with inactivity
    connection.keep_alive(true)?;

    // open a new stream and split the receiving and sending sides
    let mut stream = connection.open_bidirectional_stream().await?;
    // let (mut receive_stream, mut send_stream) = stream.split();

    println!("Client Ready");

    let (tx, mut rx) = channel::<net::KVRequestType>(1);

    let cli = thread::spawn(move || {
        std::io::stdin()
            .lines()
            .for_each(|line| match handle_input_line(line.unwrap()) {
                Some(req) => {
                    tx.blocking_send(req).unwrap();
                }
                None => (),
            })
    });

    tokio::spawn(async move {
        loop {
            if let Some(req) = rx.recv().await {
                let encoded = net::encode_request(req);
                stream.send(encoded).await.unwrap();
                if let Some(data) = stream.receive().await.unwrap() {
                    let decoded = net::decode_response(data.into()).unwrap();
                    println!("Response: {:?}", decoded);
                }
            }
        }
    })
    .await
    .unwrap();

    cli.join().unwrap();

    // spawn a task that copies responses from the server to stdout
    // tokio::spawn(async move {
    //     let mut stdout = tokio::io::stdout();
    //     let _ = tokio::io::copy(&mut receive_stream, &mut stdout).await;
    // });

    // copy data from stdin and send it to the server
    // let mut stdin = tokio::io::stdin();
    // tokio::io::copy(&mut stdin, &mut send_stream).await?;

    Ok(())
}

fn handle_input_line(line: String) -> Option<net::KVRequestType> {
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

            Some(net::KVRequestType::Put(key.to_string(), value.to_string()))
        }
        _ => {
            println!("expected GET, PUT");
            None
        }
    }
}
