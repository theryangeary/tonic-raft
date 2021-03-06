fn main() {
    tonic_build::configure()
        .compile(
            &["proto/raft/consensus.proto", "proto/example/simple.proto"],
            &["proto/raft", "proto/example"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos: {:?}", e));
}
