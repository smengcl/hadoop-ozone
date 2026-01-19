use crate::config::S3GConfig;
use crate::grpc::proto::ozone_manager_service_client::OzoneManagerServiceClient;
use thiserror::Error;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

#[derive(Debug, Error)]
pub enum OmClientError {
    #[error("no OM endpoints configured")]
    NoEndpoints,
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub struct OmGrpcClient {
    client: OzoneManagerServiceClient<Channel>,
}

impl OmGrpcClient {
    pub async fn connect(cfg: &S3GConfig) -> Result<Self, OmClientError> {
        let endpoint = cfg
            .om_endpoints
            .first()
            .ok_or(OmClientError::NoEndpoints)?;

        let mut builder = Endpoint::from_shared(format!("http://{}", endpoint))
            .map_err(|_| OmClientError::InvalidEndpoint(endpoint.clone()))?;

        if let Some(ca_path) = &cfg.tls_ca_path {
            let ca_bytes = std::fs::read(ca_path)?;
            let ca = Certificate::from_pem(ca_bytes);
            let mut tls = ClientTlsConfig::new().ca_certificate(ca);
            if let Some(domain) = &cfg.tls_domain_name {
                tls = tls.domain_name(domain.clone());
            }
            builder = builder.tls_config(tls)?;
        }

        let channel = builder.connect().await?;
        Ok(Self {
            client: OzoneManagerServiceClient::new(channel),
        })
    }

    pub fn inner_mut(&mut self) -> &mut OzoneManagerServiceClient<Channel> {
        &mut self.client
    }
}
