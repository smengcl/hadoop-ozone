use s3gateway_rs::config::S3GConfig;
use s3gateway_rs::grpc::om::OmGrpcClient;
use s3gateway_rs::grpc::proto::ozone_manager_service_server::{
    OzoneManagerService, OzoneManagerServiceServer,
};
use s3gateway_rs::grpc::proto::{OmRequest, OmResponse, Status as OmStatus, Type as OmType};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status as GrpcStatus};

#[derive(Default)]
struct TestOmService;

#[tonic::async_trait]
impl OzoneManagerService for TestOmService {
    async fn submit_request(
        &self,
        _request: Request<OmRequest>,
    ) -> Result<Response<OmResponse>, GrpcStatus> {
        let response = OmResponse {
            cmd_type: OmType::UnknownCommand as i32,
            status: OmStatus::Ok as i32,
            ..Default::default()
        };
        Ok(Response::new(response))
    }
}

#[tokio::test]
async fn om_grpc_connects_with_local_stub() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test grpc server");
    let addr = listener.local_addr().expect("grpc local addr");

    let server = tonic::transport::Server::builder()
        .add_service(OzoneManagerServiceServer::new(TestOmService::default()))
        .serve_with_incoming(TcpListenerStream::new(listener));

    let server_handle = tokio::spawn(server);

    let mut cfg = S3GConfig::default();
    cfg.om_endpoints = vec![addr.to_string()];

    let mut client = OmGrpcClient::connect(&cfg).await.expect("om connect");
    let request = OmRequest {
        cmd_type: OmType::UnknownCommand as i32,
        client_id: "test-client".to_string(),
        ..Default::default()
    };
    client
        .inner_mut()
        .submit_request(request)
        .await
        .expect("submit_request");

    server_handle.abort();
}

#[tokio::test]
#[ignore]
async fn om_grpc_connects_with_env_config() {
    let cfg = S3GConfig::load().expect("config load");
    if cfg.om_endpoints.is_empty() {
        return;
    }
    let _client = OmGrpcClient::connect(&cfg).await.expect("om connect");
}
