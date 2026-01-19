use s3gateway_rs::config::S3GConfig;
use s3gateway_rs::grpc::om::OmGrpcClient;
use s3gateway_rs::grpc::proto::ozone_manager_service_server::{
    OzoneManagerService, OzoneManagerServiceServer,
};
use s3gateway_rs::grpc::proto::{OmRequest, OmResponse, Status as OmStatus, Type as OmType};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
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

struct MiniClusterProcess {
    child: Child,
}

impl MiniClusterProcess {
    async fn start() -> Result<(Self, String), String> {
        let mut cmd = Command::new("mvn");
        cmd.arg("-q")
            .arg("-pl")
            .arg("hadoop-ozone/mini-cluster")
            .arg("-DskipTests")
            .arg("org.codehaus.mojo:exec-maven-plugin:3.1.0:java")
            .arg("-Dexec.mainClass=org.apache.hadoop.ozone.MiniOzoneClusterLauncher")
            .arg("-Dexec.classpathScope=test")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .stdin(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(|err| {
            format!("failed to spawn mini-cluster launcher via mvn: {err}")
        })?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "failed to capture launcher stdout".to_string())?;
        let mut lines = BufReader::new(stdout).lines();

        let endpoint = tokio::time::timeout(Duration::from_secs(120), async {
            loop {
                let line = lines
                    .next_line()
                    .await
                    .map_err(|err| format!("failed to read launcher output: {err}"))?;
                match line {
                    Some(line) => {
                        if let Some(value) = line.strip_prefix("OM_GRPC_ENDPOINT=") {
                            tokio::spawn(async move {
                                let mut remaining = lines;
                                while let Ok(Some(_)) = remaining.next_line().await {}
                            });
                            return Ok(value.trim().to_string());
                        }
                    }
                    None => return Err("launcher exited before providing endpoint".to_string()),
                }
            }
        })
        .await
        .map_err(|_| "timed out waiting for mini-cluster endpoint".to_string())??;

        Ok((Self { child }, endpoint))
    }

    async fn shutdown(mut self) {
        if let Some(mut stdin) = self.child.stdin.take() {
            let _ = stdin.write_all(b"shutdown\n").await;
        }
        let _ = tokio::time::timeout(Duration::from_secs(10), self.child.wait()).await;
        let _ = self.child.kill().await;
    }
}

#[tokio::test]
#[ignore]
async fn om_grpc_connects_with_env_config() {
    let cfg = S3GConfig::load().expect("config load");
    if !cfg.om_endpoints.is_empty() {
        let _client = OmGrpcClient::connect(&cfg).await.expect("om connect");
        return;
    }

    let (launcher, endpoint) = MiniClusterProcess::start()
        .await
        .expect("start mini-cluster");

    let mut local_cfg = S3GConfig::default();
    local_cfg.om_endpoints = vec![endpoint];
    let _client = OmGrpcClient::connect(&local_cfg).await.expect("om connect");

    launcher.shutdown().await;
}
