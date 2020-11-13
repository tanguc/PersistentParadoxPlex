![Build and test](https://github.com/tanguc/PersistentMarkingLB/workflows/Build%20and%20test/badge.svg?branch=master)

# persistent_marking_lb
Give us some long lived TCP stuffs, and we'll mark it for you !

Sometimes when real apps with long lived connection like TCP needs to connect
 to upstream servers.
 
Lot of back end servers are dealing with raw TCP long lived connections in
order to manage their clients and to identify them, however in the case
where we would like to have this uniquily idenfitied connections to go over
multiples stateless instance of the same back end servers, they would like
to identify all clients by a unique way (maybe a kind of identified).
    
To do that, most of clients have to change their implementations by adding
some custom metadata in their flowing frames or by using a sticky load
balancer which will keep a context for each incoming client.

We aim to support these features:

- Service Discovery (what are my upstreams servers)
- Health checking (How are my upstream servers)
- Load balancing (distribute equally stuffs)

## Features
- Unique UUID generated for downstream peers (TCP clients) and marked by us
- Round Robin like heuristic for distribution to upstreams
- gRPC connections to upstreams (which could be behind a kubernetes)
- Health checking of upstreams
- Live check of upstreams
- Dynamic upstreams peers control (for administrative purposes) via HTTP req
- Dynamic downstreams peers control (TODO) via HTTP req
- Dynamic runtime control (TODO) via HTTP req
- Protobuf generation of each languages (via google protobuf codegen)

- Ready check for upstreams
- Broadcast to all/active/inactive downstream peers
- 

## What for ?

You would like to multiplex your raw TCP connections which are long lived by
keep their context as a unique identifier and upstream them to your back-end
servers to keep the context and belongs to their original data, finally
all of them served with GRPC ! 


## Limits

Our component is not bind to any distribution or hardware specs, that's we take a
position as a `proxy-as-a-service`, in the case where all TCP connections are bound
to the end entity by some strong networking constraints(aka what CARP
& VRRP do), we do not offer a port of tcp connection/sockets between our load
balancers, once a connection is established with the a load balancer, it
will exist during the whole lifetime of the socket itself. 

Thus this load balancer is not aimed to be used as a completely stateless
component but much more as a bridge between raw TCP sockets,
tagging them and finally multiplexing them to upstreams servers.

## Recommendations

By the concept of this tool & by limitations of underlying networking concerns
we recommend to add a `Round-Robin DNS` like mechanism in the front of this
/these load(s) balancer(s), our main concerns is we would like to avoid VRRP
/CARP protocols and let external networking mechanism to handle load balancers.
Our main goal is to bring actual tools & technologies, to tie them and give a
simple solution. // NOTE the load balancer is aimed to be unique but still can be deployed multiple times, careful each load balancer have their own TCP sockets and do not share them between load balancers   
 
 ## Architecture overview
 
 ![Architecture overview](./PersistentMarkingLB_architecture_overview.png)
 

# **Development workflows**

## Start the upstream peer with supported GRPC protocol

For development purposes, it's much better to have another peer which will respond
to our messages, by sending a dummy message !

For the sake of simplicity we recommend to use the following:
https://github.com/tanguc/golang-grpc-server

Make sure to have Golang installed in your host.

To start the GRPC server:
``sh
go run main.go --port YOUR_PORT_NUMBER 
``

## How to change the protobuf file for revisions/improves ?

Protobuf's output is generated inside the sub project called `proto_gen` which is a simple cargo project which will generated Rust server/clients files from the defined protobuf file.

```bash
cd proto_gen && cargo run
cp generated/upstream.grpc.service.rs ../generated/upstream.grpc.service.rs
```

You have now the newly generated Rust file with your revisions !

## Drafts

To add:

- it's a stateful load balancer
- cannot deal by reconnecting failed tcp connections to another load balancer
- aimed to be very robust and resilient to errors (written in rust)
- GRPC very fast (for upstreams)
- best practices:
  - Better to have multiple upstreams for multiples clients (TCP)
  - 

- need to know:
  - in the case of multiples load balancers (N), upstreams will be have one connection for each load balancers (N), relation N to N, the same for load balancers (N upstreams will have N connections on each load balancer), finally N*N connections in the big picture




##### => Generated file must reside inside the "generated" folder (from protobuf file)
##### =>  