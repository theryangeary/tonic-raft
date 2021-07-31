use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tonic_raft::log::InMemoryLog;
use tonic_raft::server::RaftService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut brokers = vec![];
    let num_brokers = 3;
    let first_port = 10000;

    for broker_num in 0..num_brokers {
        // create broker list
        let mut broker_socket_addrs = vec![];
        for i in 0..num_brokers {
            broker_socket_addrs.push(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                first_port + i,
            ));
        }
        // create this broker's socket address
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            first_port + broker_num,
        );

        brokers.push(tokio::spawn(async move {
            let raft_server =
                RaftService::<InMemoryLog>::new(broker_num.into(), broker_socket_addrs);
            raft_server.router().serve(addr).await
        }));
    }

    for broker in brokers {
        let _ = broker.await?;
    }

    Ok(())
}
