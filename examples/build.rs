fn main() {
    tonic_build::configure()
        .compile(&["proto/simple/simple.proto"], &["proto/simple"])
        .unwrap_or_else(|e| panic!("Failed to compile protos: {:?}", e));
}
