fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(capnpc::CompilerCommand::new().file("net.capnp").run()?)
}
