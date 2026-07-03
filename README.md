# Bigtable Connector

Native Irodori Table connector extension for Bigtable.

This crate packages the connector metadata, native ABI exports, and driver implementation used by the Irodori extension marketplace.

## Connector

- Extension ID: `irodori.bigtable`
- Engine ID: `bigtable`
- Wire protocol: `bigtable`
- Default port: `443`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `yes`
- Marketplace visibility: `public`
- Package version: `0.1.1`

The package includes a desktop adapter source snapshot from `db/bigtable.rs`.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust crate exports the native ABI from `src/lib.rs`, uses `irodori-connector-abi` for shared JSON/buffer helpers, and keeps connector behavior in `src/driver.rs`.

## Connection Metadata

- Endpoint modes: `cloudResource`, `connectionString`
- Transport modes: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS supported: `yes`
- TLS required by default: `yes`
- Custom driver options: `yes`

### Endpoint Fields

| Field | Label | Type | Required |
| --- | --- | --- | --- |
| `projectId` | Google Cloud project | `string` | yes |
| `instanceId` | Bigtable instance | `string` | yes |

## Authentication

The connector advertises these authentication modes so clients can render the right credential fields. Driver-specific or provider-specific values can still be passed through `options` when needed.

| Auth method | Label | Kind | Secret purposes |
| --- | --- | --- | --- |
| `none` | No authentication | `none` | none |
| `connectionString` | Connection string / DSN | `connectionString` | none |
| `oauthAccessToken` | OAuth 2.0 access token | `token` | `token` |
| `serviceAccountJson` | Service account JSON | `serviceAccount` | `privateKey` |
| `serviceAccountJwt` | Service account JWT private key | `privateKey` | `privateKey`, `privateKeyPassphrase` |
| `serviceAccountImpersonation` | Service account impersonation | `iam` | `token` |
| `googleApplicationDefaultCredentials` | Application Default Credentials | `iam` | none |
| `oauth2` | OAuth 2.0 | `oauth2` | `token` |
| `workloadIdentity` | Workload identity federation | `iam` | `token` |
| `customDriverOptions` | Custom driver options | `custom` | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## Native ABI Calls

| Method | Response |
| --- | --- |
| `health` | Returns connector health, engine id, ABI version, and driver status. |
| `describe` | Returns the embedded manifest and connector config. |
| `manifest` | Returns raw `irodori.extension.json`. |
| `config` | Returns raw `connector.config.json`. |
| `connect` | Opens and validates a native connector connection. |
| `query` | Runs a connector query and returns structured rows or JSON results. |
| `metadata` | Reads schemas, tables, columns, indexes, collections, or equivalent metadata. |
| `close` | Closes and removes a cached native connection. |

## Development

All extension crates in this checkout share `../target` so dependencies compile once across sibling repositories.

```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.
