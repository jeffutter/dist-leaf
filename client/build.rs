fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(capnpc::CompilerCommand::new()
        .src_prefix("../server/")
        .file("../server/client.capnp")
        .run()?)
}
