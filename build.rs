fn main() {
    tonic_build::configure()
        .compile(&["proto/raft/consensus.proto"], &["proto/raft"])
        .unwrap_or_else(|e| panic!("Failed to compile protos: {:?}", e));
}
