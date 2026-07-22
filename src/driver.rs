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
            option_string(
                request,
                &[
                    "token",
                    "accessToken",
                    "oauthAccessToken",
                    "bearerToken",
                    "password",
                ],
            )
            .or_else(|| std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN").ok())
            .ok_or_else(|| {
                "Bigtable requires an OAuth access token or service account JSON.".to_string()
            })?
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

fn request_containers(request: &Value) -> Vec<&Value> {
    [
        Some(request),
        request.get("profile"),
        request.get("options"),
        request.get("auth"),
        request.get("secrets"),
        request
            .get("profile")
            .and_then(|profile| profile.get("options")),
        request
            .get("profile")
            .and_then(|profile| profile.get("auth")),
        request
            .get("profile")
            .and_then(|profile| profile.get("secrets")),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn option_string(request: &Value, fields: &[&str]) -> Option<String> {
    request_containers(request)
        .into_iter()
        .find_map(|container| {
            fields.iter().find_map(|field| {
                container
                    .get(*field)
                    .map(|value| match value {
                        Value::String(value) => value.clone(),
                        Value::Number(value) => value.to_string(),
                        Value::Bool(value) => value.to_string(),
                        _ => String::new(),
                    })
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
        })
}

fn push_sensitive(values: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        if !values.iter().any(|existing| existing == value) {
            values.push(value.to_string());
        }
    }
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
}
