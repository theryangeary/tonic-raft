use serde::Serialize;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tonic::{Request, Response, Status};
use tonic_raft::log::InMemoryLog;
use tonic_raft::server::RaftServer;

use tonic_raft::entry_appender::EntryAppender;

pub mod value_store {
    tonic::include_proto!("valuestore");
}

use value_store::value_store_server::{ValueStore, ValueStoreServer};
use value_store::{GetRequest, GetResponse, SetRequest, SetResponse};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker_socket_addrs = vec![];
    broker_socket_addrs.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        10000,
    ));

    // create this broker's socket address
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10000);

    let handle = tokio::spawn(async move {
        let raft_server = RaftServer::<InMemoryLog>::new(0, broker_socket_addrs);
        let entry_appender = raft_server.entry_appender();

        let simple_service = SimpleService::new(entry_appender);
        raft_server
            .router()
            .add_service(ValueStoreServer::new(simple_service))
            .serve(addr)
            .await
    });

    handle.await??;

    Ok(())
}

#[derive(Clone)]
/// A simple service that stores a single value
struct SimpleService {
    value: i32,
    entry_appender: EntryAppender,
}

impl SimpleService {
    pub fn new(entry_appender: EntryAppender) -> Self {
        Self {
            value: 0,
            entry_appender,
        }
    }
}

/// All possible stateful events
///
/// This is the datatype that will be saved entries in the event log for replicating the state
/// machine across nodes.
#[derive(Serialize)]
enum Transition {
    Set(i32),
}

#[tonic::async_trait]
impl ValueStore for SimpleService {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        println!("Got a set request from {:?}", request.remote_addr());

        let inner = request.into_inner();

        let log_entry = Transition::Set(inner.value);
        self.entry_appender
            .clone()
            .append_entry(&log_entry)
            .await
            .map_err(|e| Status::internal(e))?;

        let reply = value_store::SetResponse {};
        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        println!("Got a get request from {:?}", request.remote_addr());

        let reply = value_store::GetResponse { value: self.value };
        Ok(Response::new(reply))
    }
}
