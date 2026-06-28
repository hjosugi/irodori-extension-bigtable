# Bigtable Connector

Adds Bigtable connectivity as an installable connector extension.

This connector is listed in the public Irodori extension marketplace.

## Connector

- Extension ID: `irodori.bigtable`
- Engine ID: `bigtable`
- Wire: `bigtable`
- Default port: `443`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `false`

A desktop adapter source snapshot is staged in `native/source/` from `db/bigtable.rs`.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust code exports the native ABI plus self-description calls. Engine-specific
connect/query/metadata behavior should be linked behind `irodori_connector_call_json`.

## ABI Calls

The scaffold handles these JSON requests today:

| Method | Response |
|---|---|
| `health` / `ping` | Connector health, engine id, ABI version, and driver link status. |
| `describe` / `capabilities` | Embedded manifest and connector config. |
| `manifest` | Raw `irodori.extension.json`. |
| `config` | Raw `connector.config.json`. |

Driver operations such as `connect`, `query`, and `metadata` intentionally
return `connector.driverNotLinked` until the engine implementation is connected.

## Development

```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.
