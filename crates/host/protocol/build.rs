fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    println!("cargo:rerun-if-changed=proto/host_bridge.proto");
    let proto = ["proto/host_bridge.proto"];
    let includes = ["proto"];
    #[cfg(feature = "grpc")]
    {
        let mut config = prost_build::Config::new();
        config.protoc_executable(&protoc);
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .compile_protos_with_config(config, &proto, &includes)
            .expect("compile host bridge proto");
    }
    #[cfg(not(feature = "grpc"))]
    {
        prost_build::Config::new()
            .protoc_executable(protoc)
            .compile_protos(&proto, &includes)
            .expect("compile host bridge messages");
    }
}
