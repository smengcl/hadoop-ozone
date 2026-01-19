use std::path::PathBuf;

fn main() {
    let proto_dir = PathBuf::from("../interface-client/src/main/proto");
    let hdds_proto_dir = PathBuf::from("../../hadoop-hdds/interface-client/src/main/proto");
    let proto_file = proto_dir.join("OmClientProtocol.proto");
    let security_proto = proto_dir.join("Security.proto");
    let hdds_proto = hdds_proto_dir.join("hdds.proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());
    println!("cargo:rerun-if-changed={}", hdds_proto.display());
    println!("cargo:rerun-if-changed={}", security_proto.display());

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(
            &[proto_file, security_proto, hdds_proto],
            &[proto_dir, hdds_proto_dir],
        )
        .expect("Failed to compile Ozone OM protos");
}
