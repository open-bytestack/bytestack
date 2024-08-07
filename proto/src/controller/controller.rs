/// StackID stack id is unique id to stack which contains several data item
/// Usually stack no special means, you can think a stack as a tar file, files can be
/// archived into a stack, you can save img files or other any small files into a stack
/// then Bytestack will help you locate your file by index_id whihc format is "{i64},{offset}{cookie}"
/// This project inspirate by <https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Beaver.pdf>
/// and seaweedfs is already very very reliable blob store for small files.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StackId {
    #[prost(uint64, tag = "1")]
    pub stack_id: u64,
}
/// StackSource stores locations where the stack is.
/// locations usually are s3://xxx
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StackSource {
    #[prost(uint64, tag = "1")]
    pub stack_id: u64,
    #[prost(string, repeated, tag = "2")]
    pub locations: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CallPreLoadReq {
    #[prost(uint64, tag = "1")]
    pub stack_id: u64,
    #[prost(int64, tag = "2")]
    pub replicas: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreLoad {
    #[prost(uint64, tag = "1")]
    pub stack_id: u64,
    #[prost(enumeration = "PreLoadState", tag = "2")]
    pub state: i32,
    #[prost(int64, tag = "3")]
    pub creation_timestamp: i64,
    #[prost(string, tag = "4")]
    pub bserver: ::prost::alloc::string::String,
    #[prost(uint64, tag = "5")]
    pub size: u64,
    #[prost(uint64, tag = "6")]
    pub loaded: u64,
    #[prost(int64, tag = "7")]
    pub loaded_timestamp: i64,
    #[prost(int64, tag = "8")]
    pub update_timestamp: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreLoadAssignment {
    #[prost(uint64, tag = "1")]
    pub stack_id: u64,
    #[prost(uint64, tag = "2")]
    pub total_size: u64,
    #[prost(uint64, tag = "3")]
    pub loaded: u64,
    #[prost(string, tag = "4")]
    pub bserver: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub data_addr: ::prost::alloc::string::String,
    #[prost(int64, tag = "6")]
    pub creation_timestamp: i64,
    #[prost(string, tag = "7")]
    pub error_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreLoadAssignments {
    #[prost(message, repeated, tag = "1")]
    pub preloads: ::prost::alloc::vec::Vec<PreLoadAssignment>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PreLoadState {
    Init = 0,
    Pending = 1,
    Running = 2,
    Error = 3,
    Deleting = 4,
}
impl PreLoadState {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PreLoadState::Init => "INIT",
            PreLoadState::Pending => "PENDING",
            PreLoadState::Running => "RUNNING",
            PreLoadState::Error => "ERROR",
            PreLoadState::Deleting => "DELETING",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INIT" => Some(Self::Init),
            "PENDING" => Some(Self::Pending),
            "RUNNING" => Some(Self::Running),
            "ERROR" => Some(Self::Error),
            "DELETING" => Some(Self::Deleting),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod controller_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ControllerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ControllerClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ControllerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ControllerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ControllerClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// NextStackID try to get next_stack id from controller.
        pub async fn next_stack_id(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> std::result::Result<tonic::Response<super::StackId>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/controller.Controller/NextStackID",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("controller.Controller", "NextStackID"));
            self.inner.unary(req, path, codec).await
        }
        /// RegisterStackSource register stack_id to source.
        pub async fn register_stack_source(
            &mut self,
            request: impl tonic::IntoRequest<super::StackSource>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/controller.Controller/RegisterStackSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("controller.Controller", "RegisterStackSource"));
            self.inner.unary(req, path, codec).await
        }
        /// DeRegisterStackSource deregister source from stack_id.
        pub async fn de_register_stack_source(
            &mut self,
            request: impl tonic::IntoRequest<super::StackSource>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/controller.Controller/DeRegisterStackSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("controller.Controller", "DeRegisterStackSource"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// QueryRegisteredSource query registered source.
        pub async fn query_registered_source(
            &mut self,
            request: impl tonic::IntoRequest<super::StackId>,
        ) -> std::result::Result<tonic::Response<super::StackSource>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/controller.Controller/QueryRegisteredSource",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("controller.Controller", "QueryRegisteredSource"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// PreLoad help user to do preload or unpreload stack to bserver
        pub async fn pre_load(
            &mut self,
            request: impl tonic::IntoRequest<super::CallPreLoadReq>,
        ) -> std::result::Result<
            tonic::Response<super::PreLoadAssignments>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/controller.Controller/PreLoad",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("controller.Controller", "PreLoad"));
            self.inner.unary(req, path, codec).await
        }
        /// LocateStack help user find where the stack placed.
        pub async fn locate_stack(
            &mut self,
            request: impl tonic::IntoRequest<super::StackId>,
        ) -> std::result::Result<
            tonic::Response<super::PreLoadAssignments>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/controller.Controller/LocateStack",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("controller.Controller", "LocateStack"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod controller_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ControllerServer.
    #[async_trait]
    pub trait Controller: Send + Sync + 'static {
        /// NextStackID try to get next_stack id from controller.
        async fn next_stack_id(
            &self,
            request: tonic::Request<()>,
        ) -> std::result::Result<tonic::Response<super::StackId>, tonic::Status>;
        /// RegisterStackSource register stack_id to source.
        async fn register_stack_source(
            &self,
            request: tonic::Request<super::StackSource>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// DeRegisterStackSource deregister source from stack_id.
        async fn de_register_stack_source(
            &self,
            request: tonic::Request<super::StackSource>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// QueryRegisteredSource query registered source.
        async fn query_registered_source(
            &self,
            request: tonic::Request<super::StackId>,
        ) -> std::result::Result<tonic::Response<super::StackSource>, tonic::Status>;
        /// PreLoad help user to do preload or unpreload stack to bserver
        async fn pre_load(
            &self,
            request: tonic::Request<super::CallPreLoadReq>,
        ) -> std::result::Result<
            tonic::Response<super::PreLoadAssignments>,
            tonic::Status,
        >;
        /// LocateStack help user find where the stack placed.
        async fn locate_stack(
            &self,
            request: tonic::Request<super::StackId>,
        ) -> std::result::Result<
            tonic::Response<super::PreLoadAssignments>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ControllerServer<T: Controller> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Controller> ControllerServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ControllerServer<T>
    where
        T: Controller,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/controller.Controller/NextStackID" => {
                    #[allow(non_camel_case_types)]
                    struct NextStackIDSvc<T: Controller>(pub Arc<T>);
                    impl<T: Controller> tonic::server::UnaryService<()>
                    for NextStackIDSvc<T> {
                        type Response = super::StackId;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).next_stack_id(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = NextStackIDSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/controller.Controller/RegisterStackSource" => {
                    #[allow(non_camel_case_types)]
                    struct RegisterStackSourceSvc<T: Controller>(pub Arc<T>);
                    impl<T: Controller> tonic::server::UnaryService<super::StackSource>
                    for RegisterStackSourceSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StackSource>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).register_stack_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RegisterStackSourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/controller.Controller/DeRegisterStackSource" => {
                    #[allow(non_camel_case_types)]
                    struct DeRegisterStackSourceSvc<T: Controller>(pub Arc<T>);
                    impl<T: Controller> tonic::server::UnaryService<super::StackSource>
                    for DeRegisterStackSourceSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StackSource>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).de_register_stack_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = DeRegisterStackSourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/controller.Controller/QueryRegisteredSource" => {
                    #[allow(non_camel_case_types)]
                    struct QueryRegisteredSourceSvc<T: Controller>(pub Arc<T>);
                    impl<T: Controller> tonic::server::UnaryService<super::StackId>
                    for QueryRegisteredSourceSvc<T> {
                        type Response = super::StackSource;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StackId>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).query_registered_source(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = QueryRegisteredSourceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/controller.Controller/PreLoad" => {
                    #[allow(non_camel_case_types)]
                    struct PreLoadSvc<T: Controller>(pub Arc<T>);
                    impl<
                        T: Controller,
                    > tonic::server::UnaryService<super::CallPreLoadReq>
                    for PreLoadSvc<T> {
                        type Response = super::PreLoadAssignments;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CallPreLoadReq>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).pre_load(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PreLoadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/controller.Controller/LocateStack" => {
                    #[allow(non_camel_case_types)]
                    struct LocateStackSvc<T: Controller>(pub Arc<T>);
                    impl<T: Controller> tonic::server::UnaryService<super::StackId>
                    for LocateStackSvc<T> {
                        type Response = super::PreLoadAssignments;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StackId>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).locate_stack(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = LocateStackSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Controller> Clone for ControllerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: Controller> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Controller> tonic::server::NamedService for ControllerServer<T> {
        const NAME: &'static str = "controller.Controller";
    }
}
