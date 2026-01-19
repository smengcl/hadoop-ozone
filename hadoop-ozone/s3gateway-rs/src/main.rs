use axum::{routing::get, Router};
use s3gateway_rs::config::S3GConfig;
use tracing_subscriber::EnvFilter;

async fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = S3GConfig::load()?;
    let bind = cfg.http_bind.clone();

    let app = Router::new().route("/health", get(health));

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("s3gateway-rs listening on {}", bind);

    axum::serve(listener, app).await?;
    Ok(())
}
