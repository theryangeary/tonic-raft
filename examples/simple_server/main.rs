use serde::Serialize;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tonic::{Request, Response, Status};
use tonic_raft::log::InMemoryLog;
use tonic_raft::server::RaftService;

pub mod value_store {
    tonic::include_proto!("valuestore");
}

use value_store::value_store_server::{ValueStore, ValueStoreServer};
use value_store::{GetRequest, GetResponse, SetRequest, SetResponse};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let mut broker_socket_addrs = vec![];
    broker_socket_addrs.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        10000,
    ));
    broker_socket_addrs.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        10001,
    ));
    broker_socket_addrs.push(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        10002,
    ));

    let port = args[1].parse().unwrap();
    // create this broker's socket address
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);

    let handle = tokio::spawn(async move {
        let raft_service = RaftService::<InMemoryLog>::new(port.into(), broker_socket_addrs);

        let simple_service = SimpleService::new(raft_service.clone());

        raft_service
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
    value: Arc<AtomicU64>,
    raft_service: RaftService<InMemoryLog>,
}

impl SimpleService {
    pub fn new(raft_service: RaftService<InMemoryLog>) -> Self {
        Self {
            value: Arc::new(AtomicU64::from(0)),
            raft_service,
        }
    }

    // TODO it might make sense to make this a method of a `RaftConsumer` trait (name up for
    // debate)
    async fn apply(&self, transition: Transition) -> Result<(), String> {
        match transition {
            Transition::Set(value) => self.value.store(value, Ordering::SeqCst),
        }

        Ok(())
    }
}

/// All possible stateful events
///
/// This is the datatype that will be saved entries in the event log for replicating the state
/// machine across nodes.
#[derive(Serialize, Debug)]
enum Transition {
    Set(u64),
}

impl tonic_raft::log::Transition for Transition {}

#[tonic::async_trait]
impl ValueStore for SimpleService {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        println!("Got a set request from {:?}", request.remote_addr());

        let inner = request.into_inner();

        let transition = Transition::Set(inner.value);

        // TODO from here until "END" could potentially be one call to a `RaftConsumer` trait
        // method

        // append_transition only returns after the transition has been replicated across a
        // majority of nodes, meaning it is safe to apply
        let log_index = self
            .raft_service
            .append_transition(&transition)
            .await
            .map_err(|e| Status::internal(e))?;

        self.apply(transition)
            .await
            .map_err(|e| Status::internal(e))?;

        self.raft_service.set_last_applied(log_index);
        // "END"
        let reply = value_store::SetResponse {};
        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        println!("Got a get request from {:?}", request.remote_addr());

        let reply = value_store::GetResponse {
            value: self.value.load(Ordering::SeqCst),
        };
        Ok(Response::new(reply))
    }
}
