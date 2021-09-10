use value_store::{value_store_client::ValueStoreClient, GetRequest, SetRequest};

pub mod value_store {
    tonic::include_proto!("valuestore");
}

#[tokio::main]
/// A super-simple client for simple_server
///
/// This client will not attempt to do ANY error checking - just panic if anything unexpected
/// happens. The intention is to demonstrate to tonic-raft users the happy path.
pub async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let port = &args[1];
    // expect "set" or "get" as a command line argument
    let command = &args[2];

    // create a client to make RPCs to the server
    let mut client = ValueStoreClient::connect(format!("http://127.0.0.1:{}", port))
        .await
        .unwrap();

    if command == "set" {
        // read the value to set the server's store value to
        let value = &args[3].parse().unwrap();
        // construct the RPC request
        let request = tonic::Request::new(SetRequest { value: *value });
        // make the RPC
        let _response = client.set(request).await.unwrap();
    } else {
        // construct the RPC request
        let request = tonic::Request::new(GetRequest {});
        // make the RPC
        let response = client.get(request).await.unwrap();
        // parse the response and get the result
        let inner = response.into_inner();
        // print the result
        println!("{}", inner.value);
    }
}
