pub mod upstream_proto;

fn main() {
    println!("Compiling protobuf files into services via tonic ...");

    // Uncomment only to compile protos
    upstream_proto::compile_protos();
}
