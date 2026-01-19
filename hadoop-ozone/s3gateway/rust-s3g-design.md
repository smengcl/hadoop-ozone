# Rust Ozone S3 Gateway (gRPC OM) Design

## Summary
Build a Rust implementation of the Ozone S3 Gateway that speaks the S3 REST
API and uses Ozone Manager (OM) gRPC `submitRequest` for all namespace
operations. The initial goal is parity with the current Java S3 Gateway
behavior as documented in `hadoop-hdds/docs/content/interface/S3.md`, including
known non-compliances.

## Goals
- Implement the S3 REST subset currently supported by the Java gateway.
- Use OM gRPC (`OzoneManagerService.submitRequest`) as the sole backend
  transport.
- Preserve request/response semantics, including XML payloads and error mapping.
- Maintain operational parity: metrics, logging, TLS, configuration knobs.

## Non-goals (initial release)
- Implement Hadoop RPC in Rust.
- Add new S3 features beyond the Java gateway parity matrix.
- Replace or re-implement Ozone OM itself.

## References (existing code)
- S3 HTTP entry + filters: `hadoop-ozone/s3gateway/src/main/java/org/apache/hadoop/ozone/s3/`
- Endpoints: `hadoop-ozone/s3gateway/src/main/java/org/apache/hadoop/ozone/s3/endpoint/`
- Signature handling: `hadoop-ozone/s3gateway/src/main/java/org/apache/hadoop/ozone/s3/signature/`
- S3 secrets: `hadoop-ozone/s3gateway/src/main/java/org/apache/hadoop/ozone/s3secret/`
- OM gRPC service: `hadoop-ozone/interface-client/src/main/proto/OmClientProtocol.proto`

## Architecture

### High-level components
- **HTTP server**: Rust service exposing S3 REST APIs (path-style + virtual-host).
- **Request pipeline**: middleware for virtual host rewrite, header fixups,
  signature parsing, auth, and request identifiers.
- **OM gRPC client**: Rust client for `submitRequest` with TLS and failover.
- **Model + XML**: S3 XML request/response models, error payloads.
- **Metrics + logging**: parity with Java metrics and audit logging.

### Transport: OM gRPC client
Use `OzoneManagerService.submitRequest` from `OmClientProtocol.proto`.
The client is responsible for:
- TLS setup (trust store, server auth).
- HA failover between OMs (mirror `GrpcOMFailoverProxyProvider` behavior).
- Retrying on transient errors.
- Request envelope construction, including S3Auth metadata.

### S3Auth propagation
The Java gateway builds `S3Auth` from the signature and injects it into the OM
client thread-local (see `EndpointBase`). In Rust:
- Parse SigV4, extract AccessKey and signed headers.
- Create OMRequest with the correct user identity and S3Auth fields.
- Ensure S3Auth is included on every OM request that requires auth.

## API Parity Plan
Use `hadoop-hdds/docs/content/interface/S3.md` as the canonical API matrix.
Scope includes:
- **Bucket**: ListBuckets, CreateBucket, HeadBucket, DeleteBucket,
  GetBucketLocation.
- **Objects**: Put/Get/Head/Delete, CopyObject, ListObjectsV2, DeleteObjects.
- **Multipart**: Create, UploadPart, ListParts, Complete, Abort,
  ListMultipartUploads.
- **Tagging**: Put/Get/Delete Object Tagging.
- **Presign**: support for SigV4 query signing for core ops.
- **Known non-compliance**: match Java behavior, not AWS.

### Compatibility Matrix (Target Parity)
| Area | API | Target | Notes |
| --- | --- | --- | --- |
| Bucket | ListBuckets | ✅ | Parity with Java `RootEndpoint` |
| Bucket | CreateBucket | ✅ | Match Java non-compliance (ACL handling) |
| Bucket | HeadBucket | ✅ | Existence check only |
| Bucket | DeleteBucket | ✅ | Must be empty |
| Bucket | GetBucketLocation | ✅ | Default region behavior |
| Object | GetObject | ✅ | AWS-style errors may differ |
| Object | HeadObject | ✅ | Match Java error mapping |
| Object | PutObject | ✅ | ACL headers ignored |
| Object | DeleteObject | ✅ | Standard missing behavior |
| Object | DeleteObjects | ✅ | XML multi-delete handling |
| Object | CopyObject | ✅ | COPY/REPLACE directives |
| Object | ListObjectsV2 | ✅ | Prefix/delimiter/token support |
| Tagging | PutObjectTagging | ✅ | Replace existing tag set |
| Tagging | GetObjectTagging | ✅ | Standard XML payload |
| Tagging | DeleteObjectTagging | ✅ | Clears tag set |
| Multipart | CreateMultipartUpload | ✅ | Returns UploadId |
| Multipart | UploadPart | ✅ | ETag per part |
| Multipart | ListMultipartUploads | ✅ | Pagination |
| Multipart | ListParts | ✅ | Pagination |
| Multipart | CompleteMultipartUpload | ✅ | Multipart ETag rules |
| Multipart | AbortMultipartUpload | ✅ | Cleanup parts |
| Additional | Presigned URL | ✅ | SigV4 query signing |

## Request Pipeline (Rust)
- **Virtual host rewrite**: match `VirtualHostStyleFilter`.
- **Header preprocessor**: match `HeaderPreprocessor` fixes for AWS SDK quirks.
- **Signature parsing**: implement V4 header + query parsing; reject unsupported
  signature versions.
- **Request ID**: propagate/attach request identifier (for audit/metrics).

## Streaming and data path
- Support streaming uploads and downloads.
- Compute and validate ETag (MD5) and optional SHA256 checksum.
- Support unsigned payload variants and chunked signed payloads (for SigV4
  streaming).
- Use OM streaming APIs where available; otherwise buffer by configured size.

## Error Mapping
- Use the Java `S3ErrorTable` behavior as the source of truth.
- Map OM error codes to S3 XML error responses (status, code, message, resource).

## S3 Secret Management
- Implement `/s3secret` management endpoints with admin gating (parity with
  `s3secret` package).
- Optional: integrate remote secret store (Vault) in a follow-up.

## Configuration Parity
Implement core S3G config keys from `S3GatewayConfigKeys`:
- HTTP/HTTPS bind, ports, domain name (virtual host support).
- Client buffer size, list max keys, shallow listing.
- Auth/TLS settings, metrics configuration.

## Metrics and Auditing
- Expose metrics with compatible names and buckets where feasible.
- Audit log success/failure per operation (match existing S3G semantics).

## Deployment
- Single Rust binary + config files.
- Support TLS termination in-process or via proxy.
- Provide Docker image and example compose/k8s snippets similar to existing
  S3G deployment docs.

## Testing Strategy
- Unit tests: SigV4 parsing, canonical requests, error mapping, XML models.
- Integration: run against a local OM (docker compose), test bucket/object ops.
- Compatibility: run awscli/boto3 scripts used by existing docs.

## Milestones and Tasks

### Milestone 0: Scaffolding (1-2 weeks)
- Create Rust workspace and basic HTTP server.
- Add config loading, logging, and health endpoint.
- Generate Rust gRPC client stubs from `OmClientProtocol.proto`.

### Milestone 1: OM gRPC client (2-4 weeks)
- Implement gRPC channel management + TLS.
- Implement OM failover and retry logic.
- Build minimal `submitRequest` wrapper and test list buckets.

#### Milestone 1 Detailed Checklist
- **Proto + stubs**
  - Generate Rust stubs for `OzoneManagerService` from `OmClientProtocol.proto`.
  - Verify request/response type mapping for `OMRequest` and `OMResponse`.
- **Connection management**
  - Implement client pool keyed by OM host/port.
  - Support TLS configuration (trust store / CA certs).
  - Implement max message size parity with OM defaults.
- **Failover + retries**
  - Mirror `GrpcOMFailoverProxyProvider` behavior for HA.
  - Retry on UNAVAILABLE/TIMEOUT with bounded attempts.
  - Expose current OM target in metrics/logs.
- **S3Auth propagation**
  - Encode access ID, string-to-sign, and signature into OMRequest fields.
  - Ensure request metadata passes client IP/host info if needed by OM.
- **Minimal end-to-end**
  - Implement ListBuckets over gRPC as a smoke test.
  - Add an integration test using a local OM (compose).

### Milestone 2: Request pipeline + auth (2-4 weeks)
- Implement virtual host rewrite and header preprocessor.
- Implement SigV4 header and query parsing.
- Build S3Auth propagation into OMRequest.

#### Milestone 2 Detailed Checklist
- **Routing + middleware**
  - Virtual-host style rewrite (match `VirtualHostStyleFilter`).
  - Header preprocessor for multipart/delete quirks (match `HeaderPreprocessor`).
  - Request ID generation + propagation into logs/metrics.
- **SigV4 parsing**
  - Header-based auth (`Authorization: AWS4-HMAC-SHA256`).
  - Query-based auth (presigned URL).
  - Canonical request + string-to-sign matching Java behavior.
- **Payload handling**
  - Support unsigned payload marker.
  - Chunked signed payload parsing for streaming uploads.
- **Auth + access ID**
  - Validate access ID presence and format.
  - Reject unsupported signature versions.
  - Ensure signature info is present for every OMRequest.

### Milestone 3: Core S3 operations (4-8 weeks)
- Buckets: list, create, head, delete, location.
- Objects: put/get/head/delete, list v2, copy.
- Multipart: create, upload part, list parts, complete, abort.
- Tagging: put/get/delete.
- Errors: match Java S3ErrorTable responses.

#### Milestone 3 Detailed Checklist
- **Bucket operations**
  - ListBuckets (Root endpoint).
  - CreateBucket (validate name, map to `/s3v` volume).
  - HeadBucket + DeleteBucket.
  - GetBucketLocation response format.
- **Object operations**
  - PutObject (single PUT) with metadata + tags.
  - GetObject with range support and response header overrides.
  - HeadObject + DeleteObject.
  - CopyObject (COPY/REPLACE metadata directive).
  - ListObjectsV2 with prefix, delimiter, continuation token.
  - DeleteObjects (XML multi-delete request/response).
- **Multipart**
  - CreateMultipartUpload.
  - UploadPart (streaming).
  - ListParts (pagination).
  - CompleteMultipartUpload (assemble + final ETag).
  - AbortMultipartUpload.
  - ListMultipartUploads.
- **Tagging**
  - Put/Get/Delete Object Tagging with XML marshal/unmarshal.
- **Streaming + checksums**
  - ETag (MD5) generation for uploads.
  - Optional SHA256 validation for signed payloads.
  - Handle `Content-Length` vs streaming chunk sizes.
- **Error mapping**
  - Port `S3ErrorTable` mappings.
  - Ensure XML error response structure matches Java.

### Milestone 4: Operational parity (2-4 weeks)
- Metrics parity and audit logs.
- TLS support and config parity.
- S3 secret management endpoints.

#### Milestone 4 Detailed Checklist
- **Metrics**
  - Implement counters/latency histograms matching `S3GatewayMetrics`.
  - Expose Prometheus endpoint.
- **Audit logging**
  - Emit per-operation success/failure logs with request context.
  - Match action names in `S3GAction`.
- **Config parity**
  - Implement core keys from `S3GatewayConfigKeys`.
  - Provide default port and domain-name behavior.
- **TLS**
  - Support HTTPS listener and cert/key configuration.
  - Document TLS requirements in deployment guide.
- **S3 secret management**
  - Implement `/s3secret` generate/revoke endpoints.
  - Admin gating and error semantics parity.

### Milestone 5: Hardening + release (2-4 weeks)
- Integration tests and compatibility runs.
- Docs, examples, packaging, and release checklist.

#### Milestone 5 Detailed Checklist
- **Testing**
  - Unit tests for signature parsing and XML models.
  - Integration tests against local OM/S3G compose.
  - Compatibility tests with awscli/boto3.
- **Docs**
  - Update S3 gateway docs with Rust deployment info.
  - Document non-compliances and supported ops.
- **Packaging**
  - Docker image build + k8s manifest example.
  - Release checklist for versioning and artifacts.

## OMRequest Mapping (Derived from Java Client)
The table below maps S3 operations to OMRequest types used by the Java client
(`OzoneManagerProtocolClientSideTranslatorPB`). This is the target mapping for
the Rust gRPC client.

| S3 Operation | Java Client Call(s) | OMRequest Type(s) | Notes |
| --- | --- | --- | --- |
| ListBuckets | `OzoneVolume.listBuckets` | `ListBuckets` | Lists buckets in `/s3v` |
| CreateBucket | `ObjectStore.createS3Bucket` → `OzoneVolume.createBucket` | `CreateBucket` | May fall back to legacy layout |
| HeadBucket | `OzoneVolume.getBucket` | `InfoBucket` | Existence check |
| DeleteBucket | `ObjectStore.deleteS3Bucket` → `OzoneVolume.deleteBucket` | `DeleteBucket` | Must be empty |
| GetBucketLocation | `OzoneVolume.getBucket` (if needed) | `InfoBucket` | Java returns default region |
| ListObjectsV2 | `OzoneBucket.listKeys` / `listKeysLight` | `ListKeys` / `ListKeysLight` | Shallow listing uses Light |
| GetObject | `OzoneBucket.readKey` → `ClientProtocol.getKeyInfo` | `GetKeyInfo` | Data path uses DN streams |
| HeadObject | `OzoneBucket.getKey` → `ClientProtocol.getKeyInfo` | `GetKeyInfo` | Metadata only |
| PutObject | `OzoneBucket.createKey` | `CreateKey` + `AllocateBlock` + `CommitKey` | Data path uses SCM/DNs |
| DeleteObject | `OzoneBucket.deleteKey` | `DeleteKey` | |
| DeleteObjects | `OzoneBucket.deleteKeys` | `DeleteKeys` | Multi-delete |
| CopyObject | Get source + create dest | `GetKeyInfo` + `CreateKey` + `CommitKey` | Java may stream copy |
| CreateMultipartUpload | `OzoneBucket.initiateMultipartUpload` | `InitiateMultiPartUpload` | |
| UploadPart | `OzoneBucket.createMultipartKey` | `CreateKey` + `AllocateBlock` + `CommitMultiPartUpload` | Part commit |
| ListParts | `OzoneBucket.listParts` | `ListMultiPartUploadParts` | |
| CompleteMultipartUpload | `OzoneBucket.completeMultipartUpload` | `CompleteMultiPartUpload` | |
| AbortMultipartUpload | `OzoneBucket.abortMultipartUpload` | `AbortMultiPartUpload` | |
| ListMultipartUploads | `OzoneBucket.listMultipartUploads` | `ListMultipartUploads` | |
| PutObjectTagging | `ClientProtocol.putObjectTagging` | `PutObjectTagging` | |
| GetObjectTagging | `ClientProtocol.getObjectTagging` | `GetObjectTagging` | |
| DeleteObjectTagging | `ClientProtocol.deleteObjectTagging` | `DeleteObjectTagging` | |
| S3 Secret: Get | `ObjectStore.getS3Secret` | `GetS3Secret` | Admin-filtered |
| S3 Secret: Set | `ObjectStore.setS3Secret` | `SetS3Secret` | Admin-filtered |
| S3 Secret: Revoke | `ObjectStore.revokeS3Secret` | `RevokeS3Secret` | Admin-filtered |

Note: object data I/O still requires SCM + datanode protocols for block
allocation and streaming; OM gRPC covers namespace and key metadata only.

## Open Questions
- Do we require strict AWS behavior or only Java S3G parity?
- Which auth modes must be supported in the first release (Kerberos, simple)?
- Should S3 secret management be in v1 or deferred?
