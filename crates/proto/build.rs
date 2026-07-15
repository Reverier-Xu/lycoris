fn main() -> Result<(), Box<dyn std::error::Error>> {
  tonic_build::compile_protos("proto/node.proto")?;
  println!("cargo:rerun-if-changed=proto/node.proto");
  Ok(())
}
