#!/bin/sh

cd proto_gen && cargo build --release && ./target/release/proto_gen && cp generated/upstream.grpc.service.rs ../generated/upstream.grpc.service.rs
