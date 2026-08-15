use irodori_connector_abi::{option_string, percent_encode, push_sensitive};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

static CONNECTIONS: OnceLock<Mutex<HashMap<String, BigtableConnection>>> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Clone)]
struct BigtableConnection {
    client: Client,
    config: BigtableConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BigtableConfig {
    project_id: String,
    instance_id: String,
    access_token: String,
    data_endpoint: String,
    admin_endpoint: String,
    redaction_values: Vec<String>,
}

#[derive(Default)]
struct ObjectMeta {
    columns: Vec<Value>,
}

#[derive(Deserialize)]
struct GcpServiceAccountKey {
    project_id: String,
    client_email: String,
    private_key: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct TableListResponse {
    tables: Option<Vec<AdminTable>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct AdminTable {
    name: String,
    column_families: Option<HashMap<String, Value>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ReadRowsResponse {
    chunks: Option<Vec<CellChunk>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct CellChunk {
    row_key: Option<String>,
    family_name: Option<FamilyNameWrapper>,
    qualifier: Option<String>,
    timestamp_micros: Option<String>,
    value: Option<String>,
    value_size: Option<i32>,
    commit_row: Option<bool>,
    reset_row: Option<bool>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum FamilyNameWrapper {
    String(String),
    Object { value: String },
}

#[derive(Default)]
struct TempRow {
    row_key: String,
    cells: HashMap<String, String>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, BigtableConnection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runtime() -> Result<&'static Runtime, String> {
    if let Some(runtime) = RUNTIME.get() {
        return Ok(runtime);
    }
    let runtime = Runtime::new().map_err(|err| format!("create tokio runtime failed: {err}"))?;
    let _ = RUNTIME.set(runtime);
    RUNTIME
        .get()
        .ok_or_else(|| "create tokio runtime failed.".to_string())
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match runtime()
        .and_then(|runtime| runtime.block_on(BigtableConfig::from_request(request)))
    {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let client = Client::new();
    if let Err(err) =
        runtime().and_then(|runtime| runtime.block_on(validate_instance(&client, &config)))
    {
        return abi::error("connector.connectFailed", config.redact(&err));
    }
    let connection = BigtableConnection { client, config };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        (
            "projectId".to_string(),
            Value::String(connection.config.project_id.clone()),
        ),
        (
            "database".to_string(),
            Value::String(connection.config.instance_id.clone()),
        ),
        (
            "serverVersion".to_string(),
            Value::String("Google Cloud Bigtable v2 API".to_string()),
        ),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
        .or_else(|| abi::string_field(request, "table"))
        .or_else(|| abi::string_field(request, "tableId"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a table, tableId, sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| {
        runtime.block_on(read_rows(&connection, request, sql, abi::max_rows(request)))
    }) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match runtime().and_then(|runtime| runtime.block_on(load_metadata(&connection))) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let closed = match connections().lock() {
        Ok(mut guard) => guard.remove(&connection_id).is_some(),
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(closed)),
    ]))
}

impl BigtableConfig {
    async fn from_request(request: &Value) -> Result<Self, String> {
        let service_json = option_string(
            request,
            &["serviceAccountJson", "credentialsJson", "serviceAccountKey"],
        )
        .or_else(|| {
            option_string(request, &["password", "privateKey"])
                .filter(|value| value.trim_start().starts_with('{'))
        });
        let key = service_json
            .as_deref()
            .map(|value| {
                serde_json::from_str::<GcpServiceAccountKey>(value)
                    .map_err(|err| format!("invalid Google service account JSON: {err}"))
            })
            .transpose()?;
        let access_token = if let Some(key) = key.as_ref() {
            fetch_oauth2_token(&Client::new(), &key.client_email, &key.private_key).await?
        } else {
            let explicit = option_string(
                request,
                &[
                    "token",
                    "accessToken",
                    "oauthAccessToken",
                    "bearerToken",
                    "password",
                ],
            )
            .or_else(|| std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok());
            // Nothing supplied: fall back to Application Default Credentials
            // rather than refusing. On a developer machine that means the
            // `gcloud` login already there, and on GCE/GKE/Cloud Run the
            // metadata server, which is why ADC works with nothing configured.
            match explicit {
                Some(token) => token,
                None => {
                    fetch_adc_token(
                        &Client::new(),
                        "https://www.googleapis.com/auth/cloud-platform",
                    )
                    .await?
                }
            }
        };
        // Borrow another service account's permissions without holding its key.
        let access_token = match option_string(
            request,
            &["impersonateServiceAccount", "serviceAccountImpersonation"],
        ) {
            Some(target) => {
                let delegates: Vec<String> = option_string(request, &["impersonationDelegates"])
                    .map(|value| {
                        value
                            .split(',')
                            .map(str::trim)
                            .filter(|part| !part.is_empty())
                            .map(ToOwned::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                impersonate_service_account(
                    &Client::new(),
                    &access_token,
                    &target,
                    "https://www.googleapis.com/auth/cloud-platform",
                    &delegates,
                )
                .await?
            }
            None => access_token,
        };
        let project_id = option_string(request, &["projectId", "project"])
            .or_else(|| abi::profile_field(request, "host").map(str::to_string))
            .or_else(|| key.as_ref().map(|key| key.project_id.clone()))
            .ok_or_else(|| "Bigtable requires projectId.".to_string())?;
        let instance_id = option_string(request, &["instanceId", "instance"])
            .or_else(|| abi::profile_field(request, "database").map(str::to_string))
            .ok_or_else(|| "Bigtable requires instanceId.".to_string())?;
        let data_endpoint = option_string(request, &["dataEndpoint", "bigtableEndpoint"])
            .unwrap_or_else(|| "https://bigtable.googleapis.com".to_string());
        let admin_endpoint = option_string(request, &["adminEndpoint", "bigtableAdminEndpoint"])
            .unwrap_or_else(|| "https://bigtableadmin.googleapis.com".to_string());
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, Some(&access_token));
        Ok(Self {
            project_id,
            instance_id,
            access_token,
            data_endpoint: data_endpoint.trim_end_matches('/').to_string(),
            admin_endpoint: admin_endpoint.trim_end_matches('/').to_string(),
            redaction_values,
        })
    }

    fn redact(&self, message: &str) -> String {
        self.redaction_values
            .iter()
            .fold(message.to_string(), |message, secret| {
                if secret.is_empty() {
                    message
                } else {
                    message.replace(secret, "****")
                }
            })
    }
}

async fn validate_instance(client: &Client, config: &BigtableConfig) -> Result<(), String> {
    let url = format!(
        "{}/v2/projects/{}/instances/{}/tables?pageSize=1",
        config.admin_endpoint, config.project_id, config.instance_id
    );
    let _ = request_text(config, client.get(url)).await?;
    Ok(())
}

async fn read_rows(
    connection: &BigtableConnection,
    request: &Value,
    sql: &str,
    cap: usize,
) -> Result<QueryOutput, String> {
    let table_id = table_id_from_request(request).unwrap_or_else(|| parse_table_id(sql));
    if table_id.is_empty() {
        return Err("could not extract Bigtable table id from query.".to_string());
    }
    let mut payload = read_rows_payload(request, sql);
    if !payload.contains_key("rowsLimit") && !payload.contains_key("rows_limit") {
        payload.insert("rowsLimit".to_string(), json!(cap));
    }
    let url = format!(
        "{}/v2/projects/{}/instances/{}/tables/{}:readRows",
        connection.config.data_endpoint,
        connection.config.project_id,
        connection.config.instance_id,
        table_id
    );
    let value = request_json(
        &connection.config,
        connection.client.post(url).json(&Value::Object(payload)),
    )
    .await?;
    Ok(read_rows_response_to_output(value, cap))
}

async fn load_metadata(connection: &BigtableConnection) -> Result<Value, String> {
    let url = format!(
        "{}/v2/projects/{}/instances/{}/tables?view=SCHEMA_VIEW",
        connection.config.admin_endpoint,
        connection.config.project_id,
        connection.config.instance_id
    );
    let value = request_json(&connection.config, connection.client.get(url)).await?;
    let response = serde_json::from_value::<TableListResponse>(value)
        .map_err(|err| format!("Bigtable Admin response parse failed: {err}"))?;
    let mut objects = BTreeMap::<String, ObjectMeta>::new();
    if let Some(tables) = response.tables {
        for table in tables {
            let name = table
                .name
                .rsplit('/')
                .next()
                .unwrap_or(&table.name)
                .to_string();
            let object = objects.entry(name).or_default();
            object.columns.push(json!({
                "name": "row_key",
                "dataType": "bytes",
                "nullable": false,
                "ordinal": 1,
                "comment": "Row Key"
            }));
            if let Some(families) = table.column_families {
                for (ordinal, family) in (2..).zip(families.keys()) {
                    object.columns.push(json!({
                        "name": format!("{family}:*"),
                        "dataType": "columnFamily",
                        "nullable": true,
                        "ordinal": ordinal
                    }));
                }
            }
        }
    }
    Ok(json!({
        "schemas": [{
            "name": connection.config.instance_id,
            "objects": objects
                .into_iter()
                .map(|(name, object)| json!({
                    "schema": connection.config.instance_id,
                    "name": name,
                    "kind": "table",
                    "columns": object.columns,
                    "indexes": [],
                    "primaryKey": ["row_key"],
                    "foreignKeys": []
                }))
                .collect::<Vec<_>>()
        }]
    }))
}

fn read_rows_response_to_output(value: Value, cap: usize) -> QueryOutput {
    let responses = match value {
        Value::Array(values) => values,
        other => vec![other],
    };
    let mut temp_row = Option::<TempRow>::None;
    let mut current_family = String::new();
    let mut current_qualifier = String::new();
    let mut current_timestamp = String::new();
    let mut current_value = Vec::<u8>::new();
    let mut committed_rows = Vec::<TempRow>::new();
    let mut all_columns = BTreeSet::<String>::new();

    for response in responses {
        let Ok(response) = serde_json::from_value::<ReadRowsResponse>(response) else {
            continue;
        };
        for chunk in response.chunks.unwrap_or_default() {
            if let Some(row_key) = chunk.row_key {
                let row_key = base64_decode(&row_key)
                    .and_then(|bytes| String::from_utf8(bytes).map_err(|err| err.to_string()))
                    .unwrap_or(row_key);
                temp_row = Some(TempRow {
                    row_key,
                    cells: HashMap::new(),
                });
            }
            if let Some(family_name) = chunk.family_name {
                current_family = match family_name {
                    FamilyNameWrapper::String(value) => value,
                    FamilyNameWrapper::Object { value } => value,
                };
            }
            if let Some(qualifier) = chunk.qualifier {
                current_qualifier = base64_decode(&qualifier)
                    .and_then(|bytes| String::from_utf8(bytes).map_err(|err| err.to_string()))
                    .unwrap_or(qualifier);
            }
            if let Some(timestamp) = chunk.timestamp_micros {
                current_timestamp = timestamp;
            }
            if let Some(value) = chunk.value {
                if let Ok(decoded) = base64_decode(&value) {
                    current_value.extend(decoded);
                }
            }
            if chunk.value_size.unwrap_or(0) == 0 && !current_family.is_empty() {
                let cell_key = if current_timestamp.is_empty() {
                    format!("{}:{}", current_family, current_qualifier)
                } else {
                    format!(
                        "{}:{}@{}",
                        current_family, current_qualifier, current_timestamp
                    )
                };
                let cell_value = String::from_utf8(current_value.clone())
                    .unwrap_or_else(|_| format!("0x{}", hex_encode(&current_value)));
                current_value.clear();
                current_timestamp.clear();
                if let Some(row) = temp_row.as_mut() {
                    row.cells.insert(cell_key.clone(), cell_value);
                    all_columns.insert(cell_key);
                }
            }
            if chunk.commit_row.unwrap_or(false) {
                if let Some(row) = temp_row.take() {
                    committed_rows.push(row);
                }
            }
            if chunk.reset_row.unwrap_or(false) {
                temp_row = None;
                current_value.clear();
                current_timestamp.clear();
            }
        }
    }

    let mut columns = vec!["row_key".to_string()];
    columns.extend(all_columns);
    let truncated = committed_rows.len() > cap;
    let rows = committed_rows
        .into_iter()
        .take(cap)
        .map(|row| {
            let mut values = vec![Value::String(row.row_key)];
            values.extend(columns[1..].iter().map(|column| {
                row.cells
                    .get(column)
                    .cloned()
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            }));
            values
        })
        .collect();
    (columns, rows, truncated)
}

async fn request_json(
    config: &BigtableConfig,
    builder: reqwest::RequestBuilder,
) -> Result<Value, String> {
    let text = request_text(config, builder).await?;
    serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("Bigtable JSON response parse failed: {err}: {text}"))
}

async fn request_text(
    config: &BigtableConfig,
    builder: reqwest::RequestBuilder,
) -> Result<String, String> {
    let response = builder
        .bearer_auth(&config.access_token)
        .send()
        .await
        .map_err(|err| format!("Bigtable request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Bigtable response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("Bigtable returned HTTP {status}: {text}"));
    }
    Ok(text)
}

/// A credential file as Application Default Credentials stores it.
///
/// ADC is not one thing. `gcloud auth application-default login` writes an
/// `authorized_user` file — a refresh token, not a key — while a service
/// account downloaded from the console writes a `service_account` file, and
/// workload identity federation writes `external_account`. They need three
/// different exchanges, and reading the `type` field is the only way to know
/// which one is in front of you.
#[derive(Debug, PartialEq, Eq)]
enum AdcKind {
    ServiceAccount,
    AuthorizedUser,
    ExternalAccount,
    Unknown(String),
}

fn adc_kind(document: &Value) -> AdcKind {
    match document.get("type").and_then(Value::as_str) {
        Some("service_account") => AdcKind::ServiceAccount,
        Some("authorized_user") => AdcKind::AuthorizedUser,
        Some("external_account") => AdcKind::ExternalAccount,
        Some(other) => AdcKind::Unknown(other.to_string()),
        None => AdcKind::Unknown(String::new()),
    }
}

/// Where Application Default Credentials looks for a credential file.
///
/// `GOOGLE_APPLICATION_CREDENTIALS` first, then the well-known path
/// `gcloud auth application-default login` writes to — the same order the
/// Google client libraries use, so a machine already set up for `gcloud` needs
/// no configuration here at all.
fn adc_paths() -> Vec<String> {
    adc_paths_from(
        std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .ok()
            .as_deref(),
        std::env::var("CLOUDSDK_CONFIG").ok().as_deref(),
        std::env::var("HOME").ok().as_deref(),
    )
}

/// The search order itself, with the environment passed in.
///
/// Kept pure so it can be tested without `set_var`: the environment is
/// process-global, so env-mutating tests race each other under the default
/// parallel runner and fail in a way that looks like a logic bug.
fn adc_paths_from(
    explicit: Option<&str>,
    cloudsdk_config: Option<&str>,
    home: Option<&str>,
) -> Vec<String> {
    let mut paths = Vec::new();
    if let Some(explicit) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        paths.push(explicit.to_string());
    }
    let config_dir = cloudsdk_config
        .map(str::to_string)
        .or_else(|| home.map(|home| format!("{home}/.config/gcloud")));
    if let Some(config_dir) = config_dir {
        paths.push(format!("{config_dir}/application_default_credentials.json"));
    }
    paths
}

/// Exchange an `authorized_user` refresh token for an access token.
async fn fetch_refresh_token_grant(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> Result<String, String> {
    let body = format!(
        "grant_type=refresh_token&client_id={}&client_secret={}&refresh_token={}",
        percent_encode(client_id),
        percent_encode(client_secret),
        percent_encode(refresh_token)
    );
    let response = client
        .post("https://oauth2.googleapis.com/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| format!("Google token request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("Google token response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "Google returned HTTP {status} for the token request."
        ));
    }
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("access_token")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "Google token response contained no access_token.".to_string())
}

/// Resolve an access token from Application Default Credentials.
async fn fetch_adc_token(client: &Client, scope: &str) -> Result<String, String> {
    let mut tried = Vec::new();
    for path in adc_paths() {
        let Ok(text) = std::fs::read_to_string(&path) else {
            tried.push(path);
            continue;
        };
        let document: Value = serde_json::from_str(&text)
            .map_err(|err| format!("credential file at {path} is not valid JSON: {err}"))?;
        return match adc_kind(&document) {
            AdcKind::ServiceAccount => {
                let key: GcpServiceAccountKey =
                    serde_json::from_value(document).map_err(|err| {
                        format!("service account file at {path} is missing fields: {err}")
                    })?;
                fetch_oauth2_token(client, &key.client_email, &key.private_key).await
            }
            AdcKind::AuthorizedUser => {
                let field = |name: &str| {
                    document
                        .get(name)
                        .and_then(Value::as_str)
                        .ok_or_else(|| format!("credential file at {path} is missing {name}."))
                };
                fetch_refresh_token_grant(
                    client,
                    field("client_id")?,
                    field("client_secret")?,
                    field("refresh_token")?,
                )
                .await
            }
            // Deliberately not guessed at: external_account is workload identity
            // federation, which needs a token exchange this connector does not
            // implement. Saying so beats a confusing failure three calls later.
            AdcKind::ExternalAccount => Err(format!(
                "the credential file at {path} is a workload identity (external_account) \
                 credential, which this connector does not support yet. Use a service \
                 account key or `gcloud auth application-default login`."
            )),
            AdcKind::Unknown(kind) => Err(format!(
                "the credential file at {path} has an unrecognised credential type {kind:?}."
            )),
        };
    }

    // No file anywhere: on GCE/GKE/Cloud Run the metadata server is the
    // credential source, and it is the reason ADC works with nothing configured.
    fetch_metadata_token(client, scope).await.map_err(|err| {
        if tried.is_empty() {
            err
        } else {
            format!("{err} (no credential file at: {})", tried.join(", "))
        }
    })
}

/// Ask the GCE metadata server for a token.
async fn fetch_metadata_token(client: &Client, scope: &str) -> Result<String, String> {
    let host = std::env::var("GCE_METADATA_HOST")
        .unwrap_or_else(|_| "metadata.google.internal".to_string());
    let url = format!(
        "http://{host}/computeMetadata/v1/instance/service-accounts/default/token?scopes={}",
        percent_encode(scope)
    );
    let response = client
        .get(url)
        .header("Metadata-Flavor", "Google")
        .send()
        .await
        .map_err(|_| {
            "no Google credentials found: set GOOGLE_APPLICATION_CREDENTIALS, run \
             `gcloud auth application-default login`, or supply a service account key."
                .to_string()
        })?;
    let text = response
        .text()
        .await
        .map_err(|err| format!("metadata token response read failed: {err}"))?;
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("access_token")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "the metadata server returned no access_token.".to_string())
}

/// Exchange a token for one belonging to another service account.
///
/// This is what `--impersonate-service-account` does: the caller keeps its own
/// identity and borrows the target's permissions, so nobody has to hold the
/// target's key.
async fn impersonate_service_account(
    client: &Client,
    source_token: &str,
    target: &str,
    scope: &str,
    delegates: &[String],
) -> Result<String, String> {
    let url = format!(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{}:generateAccessToken",
        percent_encode(target)
    );
    let body = serde_json::json!({
        "scope": [scope],
        "delegates": delegates
            .iter()
            .map(|d| format!("projects/-/serviceAccounts/{d}"))
            .collect::<Vec<_>>(),
    });
    let response = client
        .post(url)
        .bearer_auth(source_token)
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("impersonation request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("impersonation response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!(
            "impersonating {target} failed with HTTP {status}. The caller needs \
             roles/iam.serviceAccountTokenCreator on that service account."
        ));
    }
    serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| {
            value
                .get("accessToken")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .ok_or_else(|| "impersonation response contained no accessToken.".to_string())
}

async fn fetch_oauth2_token(
    client: &Client,
    email: &str,
    private_key: &str,
) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let exp = now + 3600;
    let header = r#"{"alg":"RS256","typ":"JWT"}"#;
    let claims = format!(
        r#"{{"iss":"{}","scope":"https://www.googleapis.com/auth/cloud-platform","aud":"https://oauth2.googleapis.com/token","exp":{},"iat":{}}}"#,
        email, exp, now
    );
    let payload = format!(
        "{}.{}",
        base64_url_encode(header.as_bytes()),
        base64_url_encode(claims.as_bytes())
    );
    let signature = sign_rs256(private_key, payload.as_bytes())?;
    let assertion = format!("{payload}.{}", base64_url_encode(&signature));
    let body = format!(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={assertion}"
    );
    let response = client
        .post("https://oauth2.googleapis.com/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| format!("GCP token request failed: {err}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| format!("GCP token response read failed: {err}"))?;
    if !status.is_success() {
        return Err(format!("GCP token request returned HTTP {status}: {text}"));
    }
    let value = serde_json::from_str::<Value>(&text)
        .map_err(|err| format!("GCP token JSON parse failed: {err}: {text}"))?;
    value
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "GCP token response missing access_token.".to_string())
}

fn sign_rs256(private_key: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    use ring::rand::SystemRandom;
    use ring::signature::{RsaKeyPair, RSA_PKCS1_SHA256};

    let key = pem::parse(private_key)
        .map_err(|_| "invalid Google service account private key PEM.".to_string())?;
    if key.tag() != "PRIVATE KEY" {
        return Err("Google service account private key must use PKCS#8 PEM.".to_string());
    }
    let key_pair = RsaKeyPair::from_pkcs8(key.contents())
        .map_err(|_| "invalid Google service account PKCS#8 private key.".to_string())?;
    let mut signature = vec![0; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            message,
            &mut signature,
        )
        .map_err(|_| "Google service account JWT signing failed.".to_string())?;
    Ok(signature)
}

fn table_id_from_request(request: &Value) -> Option<String> {
    abi::string_field(request, "table")
        .or_else(|| abi::string_field(request, "tableId"))
        .map(clean_identifier)
}

fn parse_table_id(input: &str) -> String {
    let trimmed = input.trim();
    if let Some(pos) = trimmed.find('{') {
        return clean_identifier(trimmed[..pos].trim());
    }
    let lower = trimmed.to_ascii_lowercase();
    if let Some(from_pos) = lower.find(" from ") {
        let after_from = trimmed[from_pos + 6..].trim();
        let table_end = after_from
            .find(|c: char| c.is_whitespace() || c == ';')
            .unwrap_or(after_from.len());
        return clean_identifier(&after_from[..table_end]);
    }
    clean_identifier(trimmed)
}

fn read_rows_payload(request: &Value, sql: &str) -> Map<String, Value> {
    request
        .get("readRows")
        .or_else(|| request.get("payload"))
        .and_then(Value::as_object)
        .cloned()
        .or_else(|| {
            sql.find('{')
                .and_then(|pos| serde_json::from_str::<Value>(&sql[pos..]).ok())
                .and_then(|value| value.as_object().cloned())
        })
        .unwrap_or_default()
}

fn clean_identifier(input: &str) -> String {
    input
        .trim()
        .trim_end_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_string()
}

fn base64_url_encode(input: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i] as usize;
        let b1 = if i + 1 < input.len() {
            input[i + 1] as usize
        } else {
            0
        };
        let b2 = if i + 2 < input.len() {
            input[i + 2] as usize
        } else {
            0
        };
        out.push(CHARS[b0 >> 2] as char);
        out.push(CHARS[((b0 & 3) << 4) | (b1 >> 4)] as char);
        if i + 1 < input.len() {
            out.push(CHARS[((b1 & 15) << 2) | (b2 >> 6)] as char);
        }
        if i + 2 < input.len() {
            out.push(CHARS[b2 & 63] as char);
        }
        i += 3;
    }
    out
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut val = 0u32;
    let mut valb = -8;
    for c in input.chars() {
        if c == '=' {
            break;
        }
        let tbl = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let tbl_url = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let d = if let Some(index) = tbl.iter().position(|&value| value == c as u8) {
            index as u32
        } else if let Some(index) = tbl_url.iter().position(|&value| value == c as u8) {
            index as u32
        } else {
            continue;
        };
        val = (val << 6) | d;
        valb += 6;
        if valb >= 0 {
            out.push(((val >> valb) & 0xff) as u8);
            valb -= 8;
        }
    }
    Ok(out)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn connection(connection_id: &str) -> Result<BigtableConnection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_table_id_from_sql_or_payload_query() {
        assert_eq!(parse_table_id("SELECT * FROM `events` LIMIT 10"), "events");
        assert_eq!(parse_table_id("metrics {\"rowsLimit\":1}"), "metrics");
        assert_eq!(parse_table_id("plain_table"), "plain_table");
    }

    #[test]
    fn decodes_read_rows_response() {
        let value = json!([
            {"chunks": [
                {"rowKey": "cm93MQ==", "familyName": "cf", "qualifier": "YQ==", "value": "dmFsdWU=", "commitRow": true}
            ]}
        ]);
        let (columns, rows, truncated) = read_rows_response_to_output(value, 10);
        assert_eq!(columns, vec!["row_key", "cf:a"]);
        assert_eq!(rows[0], vec![json!("row1"), json!("value")]);
        assert!(!truncated);
    }

    #[test]
    fn encodes_base64_url_without_padding() {
        assert_eq!(base64_url_encode(b"abc"), "YWJj");
        assert_eq!(base64_url_encode(b"ab"), "YWI");
    }

    #[test]
    fn recognises_each_application_default_credential_shape() {
        // ADC is three different files needing three different exchanges, and
        // the `type` field is the only thing that says which.
        assert_eq!(
            adc_kind(&json!({ "type": "service_account", "client_email": "a@b" })),
            AdcKind::ServiceAccount
        );
        assert_eq!(
            adc_kind(&json!({ "type": "authorized_user", "refresh_token": "r" })),
            AdcKind::AuthorizedUser
        );
        assert_eq!(
            adc_kind(&json!({ "type": "external_account" })),
            AdcKind::ExternalAccount
        );
        assert_eq!(
            adc_kind(&json!({ "type": "something_new" })),
            AdcKind::Unknown("something_new".to_string())
        );
        assert_eq!(adc_kind(&json!({})), AdcKind::Unknown(String::new()));
    }

    #[test]
    fn looks_for_credentials_where_gcloud_puts_them() {
        // Matching the Google client libraries' search order means a machine
        // already set up for `gcloud` needs no configuration here.
        assert_eq!(
            adc_paths_from(
                Some("/keys/explicit.json"),
                Some("/cfg/gcloud"),
                Some("/home/u")
            ),
            vec![
                "/keys/explicit.json".to_string(),
                "/cfg/gcloud/application_default_credentials.json".to_string(),
            ]
        );
        // Without CLOUDSDK_CONFIG the well-known path under HOME is used.
        assert_eq!(
            adc_paths_from(None, None, Some("/home/u")),
            vec!["/home/u/.config/gcloud/application_default_credentials.json".to_string()]
        );
        // Nothing to go on: the caller falls through to the metadata server.
        assert!(adc_paths_from(None, None, None).is_empty());
    }

    #[test]
    fn an_empty_credentials_variable_is_not_a_path() {
        // An exported-but-empty variable is common in shell profiles and would
        // otherwise send the search to "".
        assert_eq!(
            adc_paths_from(Some("   "), Some("/cfg"), None),
            vec!["/cfg/application_default_credentials.json".to_string()]
        );
    }

    #[test]
    fn form_encoding_protects_the_grant_body() {
        // A refresh token or a service account email in a form body must not be
        // able to introduce another parameter.
        assert_eq!(percent_encode("a&b=c"), "a%26b%3Dc");
        assert_eq!(
            percent_encode("svc@project.iam.gserviceaccount.com"),
            "svc%40project.iam.gserviceaccount.com"
        );
        assert_eq!(percent_encode("plain-Token_1.0~"), "plain-Token_1.0~");
    }
}
