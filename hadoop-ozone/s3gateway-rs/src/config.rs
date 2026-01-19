use serde::Deserialize;
use std::env;
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize)]
pub struct S3GConfig {
    pub http_bind: String,
    pub om_endpoints: Vec<String>,
    pub tls_ca_path: Option<String>,
    pub tls_domain_name: Option<String>,
}

impl Default for S3GConfig {
    fn default() -> Self {
        Self {
            http_bind: "0.0.0.0:9878".to_string(),
            om_endpoints: Vec::new(),
            tls_ca_path: None,
            tls_domain_name: None,
        }
    }
}

impl S3GConfig {
    pub fn load() -> Result<Self, config::ConfigError> {
        let mut builder = config::Config::builder()
            .set_default("http_bind", "0.0.0.0:9878")?
            .set_default("om_endpoints", Vec::<String>::new())?;

        if let Ok(path) = env::var("S3G_CONFIG_PATH") {
            builder = builder.add_source(config::File::from(PathBuf::from(path)));
        }

        builder = builder.add_source(
            config::Environment::with_prefix("S3G")
                .separator("__")
                .try_parsing(true),
        );

        let mut cfg: S3GConfig = builder.build()?.try_deserialize()?;

        if let Ok(raw_endpoints) = env::var("S3G_OM_ENDPOINTS") {
            cfg.om_endpoints = raw_endpoints
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        Ok(cfg)
    }
}
