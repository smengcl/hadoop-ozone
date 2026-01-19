# s3gateway-rs (scaffold)

This is the initial Rust scaffold for an Ozone S3 Gateway using OM gRPC.

## Build
```bash
cargo build
```

## Run
```bash
S3G_HTTP_BIND=0.0.0.0:9878 \
S3G_OM_ENDPOINTS=omhost:9871 \
cargo run
```

## Health check
```bash
curl http://localhost:9878/health
```

## Notes
- gRPC stubs are generated from `OmClientProtocol.proto` at build time.
- OM endpoints should be provided as `host:port` entries.
- TLS configuration is supported via:
  - `S3G_TLS_CA_PATH`
  - `S3G_TLS_DOMAIN_NAME`
