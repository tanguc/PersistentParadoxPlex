use crate::downstream::{PeerEventRxChannel, PeerEventTxChannel};
use crate::runtime::{RuntimeEvent, RuntimeOrderRxChannel, RuntimeOrderTxChannel};
use futures_core::Stream;
use std::path::PathBuf;
use std::pin::Pin;
use tokio;

static OUT_DIR: &str = "./generated";

pub fn compile_protos() {
    std::env::set_var("OUT_DIR", OUT_DIR);

    let generated_folder = PathBuf::from(OUT_DIR);
    if !generated_folder.exists() {
        debug!(
            "Creating the folder [{}] to store generated proto files",
            generated_folder.clone().display().to_string()
        );
        if let Err(err) = std::fs::create_dir_all(generated_folder.clone()) {
            panic!(
                "Failed to create the folder [{:?}]",
                generated_folder.clone().display()
            );
        }
    }

    let protofile_path = "misc/grpc/proto/upstream.proto";
    let path = PathBuf::from(&protofile_path);
    if path.exists() {
        tonic_build::compile_protos(path).unwrap_or_else(|e| panic!("Failed to compile {:?}", e));
    } else {
        panic!(
            "The protofile [{}] does not exist. Please check before trying to compile",
            protofile_path
        );
    }
}

include!("../generated/upstream.grpc.service.rs");

pub struct UpstreamPeerServiceImpl {
    rt_tx: RuntimeOrderTxChannel, // to send messages to downstream (frontend peeers)
    rt_rx: RuntimeOrderRxChannel, // to receive stop order (close connection with backeend peer)...
    backend_peer_tx: PeerEventTxChannel, // channel to use on runtime to send a new message to upstream (backend peer)
    backend_peer_rx: PeerEventRxChannel, // channel to receive message to sink
}

// /// DEBUGGING PURPOSES
// #[tonic::async_trait]
// impl UpstreamPeerService for UpstreamPeerServiceImpl {
//     type bidirectionalStreamingStream = Pin<
//         Box<dyn Stream<Item = Result<OutputStreamRequest, tonic::Status>> + Send + Sync + 'static>,
//     >;

// async fn bidirectional_streaming(
//     &self,
//     mut request: tonic::Request<tonic::Streaming<InputStreamRequest>>,
// ) -> Result<tonic::Response<Self::bidirectionalStreamingStream>, tonic::Status> {
// let mut rt_tx = self.rt_tx.clone();
// tokio::spawn(async move {
//     debug!("starting streaming responses to downstream peer");
//     loop {
//         // let mut request = request as mut;
//         let backend_peer_message = request.get_mut()
//             .message().await?.ok_or_else(|| {
//             tonic::Status::internal("Could not read message 9384")
//         })?;

//         // let backend_peer_message: &mut tonic::Streaming<InputStreamRequest> =
//         //     request.get_mut();
//         // backend_peer_message: Result<Option<InputStreamRequest>, tonic::Status> =
//         //     backend_peer_message.message().await;
//         // match foo {
//         //     backend_peer_message => {
//         debug!("Got backend peer message [{:?}]", backend_peer_message);

//         // notify the runtime we got a new message to pass it though frontend peers
//         if let Err(_) = rt_tx.send(
//             RuntimeEvent::GotMessageFromUpstreamPeer(
//                 backend_peer_message.payload)).await {
//             error!("cannot send received message to frontend peer");
//         }
//         debug!("message send to frontpeer");
//         //     }
//         //     // None => {
//         //     //     error!("Could not get message from upstream (backeend peer)");
//         //     // }
//         // }

//     };

//     Result::<(), tonic::Status>::Ok(())
// });

// let mut rt_rx = self.rt_rx.clone();
// let foo = async_stream::try_stream! {
//     loop {
//         if let Some(rcv_order) = rt_rx.recv().await {
//             match rcv_order {
//                 RuntimeEvent::GotMessageFromDownstream(str) => {
//                     yield OutputStreamRequest {
//                         header: Option::None,
//                         payload: str
//                     };
//                 },
//                 RuntimeEvent::ShutdownPeer => {
//                     info!("Closing the upstream backend peer");
//                     break;
//                 },
//                 _ => {
//                     info!("Got a weirdo runtime order");
//                 }

//             }
//         } else {
//             error!("Failed to receive order from runtime channel");
//         }
//     }
// };

// Ok(tonic::Response::new(
//     Box::pin(foo)
// ))

// unimplemented!()
// }
// }
//
