use tonic::transport::Server;

pub mod backend_peer {
    tonic::include_proto!("backendpeer");
}

use backend_peer::backend_peer_service_server::BackendPeerService;
use backend_peer::{
    InputStreamRequest,
    OutputStreamRequest
};
use std::pin::Pin;
use futures_core::Stream;

pub fn init_tonic() {
    tonic_build::compile_protos("proto/backend_peer.proto")
        .unwrap_or_else(|e| panic!("Failed to compile {:?}", e));
}

#[derive(Debug)]
pub struct BackendPeer;

#[tonic::async_trait]
impl BackendPeerService for BackendPeer {

    type bidirectionalStreamingStream =
     Pin<Box<dyn Stream<Item = Result<OutputStreamRequest, tonic::Status>> + Send + Sync + 'static>>;

    async fn bidirectional_streaming(
        &self,
        request: tonic::Request<tonic::Streaming<InputStreamRequest>>
    ) -> Result<tonic::Response<Self::bidirectionalStreamingStream>, tonic::Status> {
        unimplemented!()
    }
}
