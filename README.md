# PersistentMarkingLB
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



## What for ?

When your 

## Limits

This load balancer is not aimed to be used as a stateless 

## Recommendations

By the concept of this tool & by limitations of underlying networking concerns
we recommend to add a `Round-Robin DNS` like mechanism in the front of this
/these load(s) balancer(s), our main concerns is we would like to avoid VRRP
/CARP protocols and let external networking mechanism to handle load balancers.
Our main goal is to bring actual tools & technologies, to tie them and give a
 simple solution.
 
 ## Architecture overview
 
 ![Architecture overview](./PersistentMarkingLB_architecture_overview.png)
 