fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(capnpc::CompilerCommand::new().file("server.capnp").run()?)
}
