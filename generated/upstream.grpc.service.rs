#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadyRequest {
    #[prost(message, optional, tag = "1")]
    pub header: ::std::option::Option<Header>,
    /// ^^^^^^
    /// ||||||
    /// because https://github.com/tokopedia/gripmock/ have been coded by looser
    /// and they cant handle match of regexp on anything other than string, you
    /// sucks. TODO delete when wel'll get ride out of these tools.
    #[prost(string, tag = "2")]
    pub ready: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReadyResult {
    #[prost(message, optional, tag = "1")]
    pub header: ::std::option::Option<Header>,
    #[prost(bool, tag = "2")]
    pub ready: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LiveRequest {
    #[prost(message, optional, tag = "1")]
    pub header: ::std::option::Option<Header>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LiveResult {
    #[prost(message, optional, tag = "1")]
    pub header: ::std::option::Option<Header>,
    #[prost(bool, tag = "2")]
    pub live: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Header {
    #[prost(string, tag = "1")]
    pub address: std::string::String,
    #[prost(string, tag = "2")]
    pub time: std::string::String,
    #[prost(string, tag = "3")]
    pub client_uuid: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InputStreamRequest {
    #[prost(message, optional, tag = "1")]
    pub header: ::std::option::Option<Header>,
    #[prost(string, tag = "2")]
    pub payload: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutputStreamRequest {
    #[prost(message, optional, tag = "1")]
    pub header: ::std::option::Option<Header>,
    #[prost(string, tag = "2")]
    pub payload: std::string::String,
}
#[doc = r" Generated client implementations."]
pub mod upstream_peer_service_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct UpstreamPeerServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl UpstreamPeerServiceClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> UpstreamPeerServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn bidirectional_streaming(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::InputStreamRequest>,
        ) -> Result<
            tonic::Response<tonic::codec::Streaming<super::OutputStreamRequest>>,
            tonic::Status,
        > {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/upstream.grpc.service.UpstreamPeerService/bidirectionalStreaming",
            );
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
        pub async fn ready(
            &mut self,
            request: impl tonic::IntoRequest<super::ReadyRequest>,
        ) -> Result<tonic::Response<super::ReadyResult>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/upstream.grpc.service.UpstreamPeerService/ready",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn live(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::LiveRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<super::LiveResult>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/upstream.grpc.service.UpstreamPeerService/live",
            );
            self.inner
                .streaming(request.into_streaming_request(), path, codec)
                .await
        }
    }
    impl<T: Clone> Clone for UpstreamPeerServiceClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for UpstreamPeerServiceClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "UpstreamPeerServiceClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod upstream_peer_service_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with UpstreamPeerServiceServer."]
    #[async_trait]
    pub trait UpstreamPeerService: Send + Sync + 'static {
        #[doc = "Server streaming response type for the bidirectionalStreaming method."]
        type bidirectionalStreamingStream: Stream<Item = Result<super::OutputStreamRequest, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn bidirectional_streaming(
            &self,
            request: tonic::Request<tonic::Streaming<super::InputStreamRequest>>,
        ) -> Result<tonic::Response<Self::bidirectionalStreamingStream>, tonic::Status>;
        async fn ready(
            &self,
            request: tonic::Request<super::ReadyRequest>,
        ) -> Result<tonic::Response<super::ReadyResult>, tonic::Status>;
        #[doc = "Server streaming response type for the live method."]
        type liveStream: Stream<Item = Result<super::LiveResult, tonic::Status>>
            + Send
            + Sync
            + 'static;
        async fn live(
            &self,
            request: tonic::Request<tonic::Streaming<super::LiveRequest>>,
        ) -> Result<tonic::Response<Self::liveStream>, tonic::Status>;
    }
    #[derive(Debug)]
    #[doc(hidden)]
    pub struct UpstreamPeerServiceServer<T: UpstreamPeerService> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: UpstreamPeerService> UpstreamPeerServiceServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for UpstreamPeerServiceServer<T>
    where
        T: UpstreamPeerService,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/upstream.grpc.service.UpstreamPeerService/bidirectionalStreaming" => {
                    #[allow(non_camel_case_types)]
                    struct bidirectionalStreamingSvc<T: UpstreamPeerService>(pub Arc<T>);
                    impl<T: UpstreamPeerService>
                        tonic::server::StreamingService<super::InputStreamRequest>
                        for bidirectionalStreamingSvc<T>
                    {
                        type Response = super::OutputStreamRequest;
                        type ResponseStream = T::bidirectionalStreamingStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::InputStreamRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.bidirectional_streaming(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = bidirectionalStreamingSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/upstream.grpc.service.UpstreamPeerService/ready" => {
                    #[allow(non_camel_case_types)]
                    struct readySvc<T: UpstreamPeerService>(pub Arc<T>);
                    impl<T: UpstreamPeerService> tonic::server::UnaryService<super::ReadyRequest> for readySvc<T> {
                        type Response = super::ReadyResult;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReadyRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.ready(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = readySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/upstream.grpc.service.UpstreamPeerService/live" => {
                    #[allow(non_camel_case_types)]
                    struct liveSvc<T: UpstreamPeerService>(pub Arc<T>);
                    impl<T: UpstreamPeerService> tonic::server::StreamingService<super::LiveRequest> for liveSvc<T> {
                        type Response = super::LiveResult;
                        type ResponseStream = T::liveStream;
                        type Future =
                            BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::LiveRequest>>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { inner.live(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1;
                        let inner = inner.0;
                        let method = liveSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: UpstreamPeerService> Clone for UpstreamPeerServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: UpstreamPeerService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: UpstreamPeerService> tonic::transport::NamedService for UpstreamPeerServiceServer<T> {
        const NAME: &'static str = "upstream.grpc.service.UpstreamPeerService";
    }
}
