# s3gateway-rs

Rust implementation of the Ozone S3 Gateway that talks to Ozone Manager (OM)
over gRPC. This submodule is a scaffold with a health endpoint, configuration
loading, and OM gRPC client plumbing.

Design doc: `hadoop-ozone/s3gateway/rust-s3g-design.md`.

## Build
```bash
cd hadoop-ozone/s3gateway-rs
cargo build
```

## Run
```bash
cd hadoop-ozone/s3gateway-rs
S3G_HTTP_BIND=0.0.0.0:9878 \
S3G_OM_ENDPOINTS=omhost:8981 \
cargo run
```

Config is loaded from defaults, optional config file, then env overrides:
- `S3G_CONFIG_PATH` (path to config file)
- `S3G_HTTP_BIND`
- `S3G_OM_ENDPOINTS` (comma-separated `host:port`)
- `S3G_TLS_CA_PATH`
- `S3G_TLS_DOMAIN_NAME`

## Health check
```bash
curl http://localhost:9878/health
```

## Tests
```bash
cd hadoop-ozone/s3gateway-rs
cargo test
```

The ignored smoke test can also spin up a MiniOzoneCluster if no OM endpoints
are configured. It requires Maven on `PATH`:
```bash
cd hadoop-ozone/s3gateway-rs
cargo test --test om_smoke -- --ignored
```

## Notes
- gRPC stubs are generated from `OmClientProtocol.proto` at build time.
- `protoc` is required for builds (`brew install protobuf` on macOS).
