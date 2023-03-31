fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(capnpc::CompilerCommand::new()
        .src_prefix("../quic-server/")
        .file("../quic-server/client.capnp")
        .run()?)
}
