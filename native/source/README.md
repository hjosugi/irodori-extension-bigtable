# Native Source

The initial source snapshot was copied from `db/bigtable.rs` in the desktop app.

Source SHA-256: `493122b104ea77074c83ab023a4643fa74832816566b9c5263072f4087217822`.


This directory is a migration staging area for `irodori.bigtable`. The active native
ABI shim lives in `src/lib.rs`; engine-specific connect/query/metadata behavior
should move here as the connector runtime contract is wired into the desktop app.

Engine status from `knowledge/engines.json`: `wired`.
