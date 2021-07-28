# Self Contained Cluster

This example demonstrates how to create 5 raft brokers in a single binary, so
that clustering can be easy performed without needing multiple nodes,
containers, or container orchestration. An absolute bare minimum tonic-raft
cluster.

There is no consuming application here, so the raft log will never have any
logs added to it.
