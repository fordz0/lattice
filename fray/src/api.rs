use crate::blocklist::ContentBlocklist;
use crate::directory::{FrayDirectory, FrayDirectoryEntry, FrayStatus, SignedFrayDirectory};
use crate::model::{Comment, CreateCommentRequest, CreatePostRequest, Post};
use crate::network;
use crate::store::FrayStore;
use crate::trust::{
    sign_trust_record, unix_ts, FrayTrustRecord, KeyRecord, KeyStanding, SignedTrustRecord,
};
use crate::ui;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::{Signature, SigningKey, Verifier};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;

const MAX_JSON_BODY_BYTES: usize = 64 * 1024;
const FEED_POST_LIMIT: usize = 20;
const FEED_COMMENTS_PER_POST_LIMIT: usize = 10;

#[derive(Clone)]
pub struct AppState {
    pub store: FrayStore,
    pub lattice_rpc_port: u16,
    pub signing_key: Arc<SigningKey>,
    pub blocklist: ContentBlocklist,
    pub blocklist_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ClaimFrayRequest {
    owner_key_b64: String,
}

#[derive(Debug, Deserialize)]
struct StandingRequest {
    key_b64: String,
    standing: String,
    label: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ModeratorRequest {
    key_b64: String,
}

#[derive(Debug, Deserialize)]
struct DirectoryEntryRequest {
    fray_name: String,
    owner_key_b64: String,
    status: String,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BlocklistRequest {
    hash_hex: String,
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui_index))
        .route("/directory", get(ui_index))
        .route("/f/{fray}", get(ui_index))
        .route("/f/{fray}/mod", get(ui_index))
        .route("/f/{fray}/{post_id}", get(ui_index))
        .route("/health", get(health))
        .route("/api/v1/info", get(info))
        .route("/api/v1/directory", get(get_directory))
        .route("/api/v1/directory/sync", post(sync_directory))
        .route("/api/v1/directory/entries", post(upsert_directory_entry))
        .route(
            "/api/v1/directory/entries/{fray_name}",
            delete(ban_directory_entry),
        )
        .route(
            "/api/v1/frays/{fray}/posts",
            get(list_posts).post(create_post),
        )
        .route("/api/v1/frays/{fray}/posts/{post_id}", get(get_post))
        .route(
            "/api/v1/frays/{fray}/posts/{post_id}/comments",
            get(list_comments).post(create_comment),
        )
        .route("/api/v1/frays/{fray}/claim", post(claim_fray))
        .route("/api/v1/frays/{fray}/trust", get(get_trust_record))
        .route(
            "/api/v1/frays/{fray}/trust/standings",
            post(set_trust_standing),
        )
        .route(
            "/api/v1/frays/{fray}/trust/moderators",
            post(add_moderator),
        )
        .route(
            "/api/v1/frays/{fray}/trust/moderators/{key_b64}",
            delete(remove_moderator),
        )
        .route("/api/v1/frays/{fray}/sync/publish", post(publish_fray))
        .route("/api/v1/frays/{fray}/sync/pull", post(pull_fray))
        .route("/api/v1/admin/blocklist", post(add_blocklist_hash))
        .route("/api/f/{fray}/posts", get(list_posts).post(create_post))
        .route("/api/f/{fray}/posts/{post_id}", get(get_post))
        .route(
            "/api/f/{fray}/posts/{post_id}/comments",
            get(list_comments).post(create_comment),
        )
        .route("/api/f/{fray}/sync/publish", post(publish_fray))
        .route("/api/f/{fray}/sync/pull", post(pull_fray))
        .layer(DefaultBodyLimit::max(MAX_JSON_BODY_BYTES))
        .with_state(state)
}

async fn ui_index() -> Response {
    let mut response = axum::response::Html(ui::page_html()).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
}

async fn health() -> impl IntoResponse {
    Json(json!({"status":"ok"}))
}

async fn info(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
      "service": "fray",
      "api_version": "v1",
      "lattice_rpc_port": state.lattice_rpc_port,
      "ui_routes": ["/", "/directory", "/f/{fray}", "/f/{fray}/mod", "/f/{fray}/{post_id}"],
      "routes": [
        "GET /api/v1/info",
        "GET /api/v1/directory",
        "POST /api/v1/directory/sync",
        "POST /api/v1/frays/{fray}/claim",
        "GET /api/v1/frays/{fray}/trust",
        "POST /api/v1/frays/{fray}/trust/standings",
        "POST /api/v1/frays/{fray}/trust/moderators",
        "DELETE /api/v1/frays/{fray}/trust/moderators/{key_b64}",
        "GET /api/v1/frays/{fray}/posts",
        "POST /api/v1/frays/{fray}/posts",
        "GET /api/v1/frays/{fray}/posts/{post_id}",
        "GET /api/v1/frays/{fray}/posts/{post_id}/comments",
        "POST /api/v1/frays/{fray}/posts/{post_id}/comments",
        "POST /api/v1/frays/{fray}/sync/publish",
        "POST /api/v1/frays/{fray}/sync/pull",
        "POST /api/v1/admin/blocklist"
      ]
    }))
}

async fn create_post(
    State(state): State<AppState>,
    Path(fray): Path<String>,
    Json(request): Json<CreatePostRequest>,
) -> Response {
    match state.store.create_post(&fray, request) {
        Ok(post) => (StatusCode::CREATED, Json(post_response(&state.store, &fray, &post))).into_response(),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn list_posts(
    State(state): State<AppState>,
    Path(fray): Path<String>,
    Query(query): Query<ListQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(50);
    match state.store.list_posts_full(&fray, limit) {
        Ok(posts) => {
            let posts = posts
                .iter()
                .map(|post| post_summary_response(&state.store, &fray, post))
                .collect::<Vec<_>>();
            Json(json!({ "posts": posts })).into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn get_post(
    State(state): State<AppState>,
    Path((fray, post_id)): Path<(String, String)>,
) -> Response {
    match state.store.get_post(&fray, &post_id) {
        Ok(Some(post)) => Json(post_response(&state.store, &fray, &post)).into_response(),
        Ok(None) => not_found("post not found"),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn create_comment(
    State(state): State<AppState>,
    Path((fray, post_id)): Path<(String, String)>,
    Json(request): Json<CreateCommentRequest>,
) -> Response {
    match state.store.create_comment(&fray, &post_id, request) {
        Ok(comment) => {
            (StatusCode::CREATED, Json(comment_response(&state.store, &fray, &comment))).into_response()
        }
        Err(err) if err.to_string() == "post not found" => not_found("post not found"),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn list_comments(
    State(state): State<AppState>,
    Path((fray, post_id)): Path<(String, String)>,
    Query(query): Query<ListQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(200);
    match state.store.list_comments_full(&fray, &post_id, limit) {
        Ok(comments) => {
            let comments = comments
                .iter()
                .map(|comment| comment_response(&state.store, &fray, comment))
                .collect::<Vec<_>>();
            Json(json!({ "comments": comments })).into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn claim_fray(
    State(state): State<AppState>,
    Path(fray): Path<String>,
    Json(request): Json<ClaimFrayRequest>,
) -> Response {
    match network::check_frayloom_stake(state.lattice_rpc_port).await {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "you must have fray.loom pinned and trusted to claim a fray" })),
            )
                .into_response()
        }
        Err(err) => return bad_gateway(err.to_string()),
    }

    if let Ok(Some(existing)) = state.store.get_fray_ownership(&fray) {
        if existing != request.owner_key_b64 {
            return (StatusCode::CONFLICT, Json(json!({ "error": "fray is already claimed" })))
                .into_response();
        }
    }

    let local_key = signing_key_b64(state.signing_key.as_ref());
    if request.owner_key_b64 != local_key {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "claim owner key must match the local signing key" })),
        )
            .into_response();
    }

    if let Err(err) = state.store.store_fray_ownership(&fray, &request.owner_key_b64) {
        return bad_request(err.to_string());
    }

    let trust_record = FrayTrustRecord {
        version: 1,
        fray: fray.clone(),
        owner_key_b64: request.owner_key_b64.clone(),
        moderator_keys: Vec::new(),
        entries: Vec::new(),
        generated_at: unix_ts(),
    };
    let signed = match sign_trust_record(&trust_record, state.signing_key.as_ref()) {
        Ok(record) => record,
        Err(err) => return bad_request(err.to_string()),
    };
    if let Err(err) = state.store.store_trust_record(&fray, &signed) {
        return bad_request(err.to_string());
    }
    if let Err(err) = network::publish_trust_record(
        &fray,
        &trust_record,
        state.signing_key.as_ref(),
        state.lattice_rpc_port,
    )
    .await
    {
        return bad_gateway(err.to_string());
    }

    Json(json!({ "status": "ok", "fray": fray })).into_response()
}

async fn get_trust_record(State(state): State<AppState>, Path(fray): Path<String>) -> Response {
    match state.store.load_trust_record(&fray) {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => not_found("trust record not found"),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn set_trust_standing(
    State(state): State<AppState>,
    Path(fray): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request: StandingRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(err) => return bad_request(format!("invalid json: {err}")),
    };
    let mut record = match load_owned_trust_record(&state, &fray) {
        Ok(record) => record,
        Err(response) => return response,
    };
    let authorized_keys = moderator_authorized_keys(&record.record);
    if let Err(err) = verify_body_signature(&headers, &body, &authorized_keys) {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": err }))).into_response();
    }

    let standing = match parse_standing(&request.standing, request.reason.clone()) {
        Ok(standing) => standing,
        Err(err) => return bad_request(err),
    };
    let now = unix_ts();
    upsert_key_record(
        &mut record.record.entries,
        KeyRecord {
            key_b64: request.key_b64.clone(),
            standing: standing.clone(),
            label: request.label,
            updated_at: now,
        },
    );
    record.record.generated_at = now;
    record.record.version = record.record.version.saturating_add(1);
    persist_trust_record(&state, &fray, &record).await
}

async fn add_moderator(
    State(state): State<AppState>,
    Path(fray): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request: ModeratorRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(err) => return bad_request(format!("invalid json: {err}")),
    };
    let mut record = match load_owned_trust_record(&state, &fray) {
        Ok(record) => record,
        Err(response) => return response,
    };
    let owner_only = vec![record.record.owner_key_b64.clone()];
    if let Err(err) = verify_body_signature(&headers, &body, &owner_only) {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": err }))).into_response();
    }
    if !record.record.moderator_keys.contains(&request.key_b64) {
        if record.record.moderator_keys.len() >= 32 {
            return bad_request("too many moderators".to_string());
        }
        record.record.moderator_keys.push(request.key_b64);
    }
    record.record.generated_at = unix_ts();
    record.record.version = record.record.version.saturating_add(1);
    persist_trust_record(&state, &fray, &record).await
}

async fn remove_moderator(
    State(state): State<AppState>,
    Path((fray, key_b64)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut record = match load_owned_trust_record(&state, &fray) {
        Ok(record) => record,
        Err(response) => return response,
    };
    let owner_only = vec![record.record.owner_key_b64.clone()];
    if let Err(err) = verify_body_signature(&headers, &body, &owner_only) {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": err }))).into_response();
    }
    record.record.moderator_keys.retain(|value| value != &key_b64);
    record.record.generated_at = unix_ts();
    record.record.version = record.record.version.saturating_add(1);
    persist_trust_record(&state, &fray, &record).await
}

async fn publish_fray(State(state): State<AppState>, Path(fray): Path<String>) -> Response {
    let posts = match state.store.list_posts_full(&fray, FEED_POST_LIMIT) {
        Ok(posts) => posts,
        Err(err) => return bad_request(err.to_string()),
    };
    let post_ids: Vec<String> = posts.iter().map(|p| p.id.clone()).collect();
    let comments =
        match state
            .store
            .collect_comments_for_posts(&fray, &post_ids, FEED_COMMENTS_PER_POST_LIMIT)
        {
            Ok(comments) => comments,
            Err(err) => return bad_request(err.to_string()),
        };
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => return bad_request("system clock before unix epoch".to_string()),
    };

    match network::publish_feed(
        state.lattice_rpc_port,
        state.signing_key.as_ref(),
        &fray,
        posts.clone(),
        comments.clone(),
        now,
    )
    .await
    {
        Ok(()) => Json(json!({
            "status": "ok",
            "fray": fray,
            "published_posts": posts.len(),
            "published_comments": comments.len(),
            "record_key": format!("app:fray:feed:{}", fray),
        }))
        .into_response(),
        Err(err) => bad_gateway(err.to_string()),
    }
}

async fn pull_fray(State(state): State<AppState>, Path(fray): Path<String>) -> Response {
    let signed_feed = match network::fetch_feed(state.lattice_rpc_port, &fray).await {
        Ok(Some(feed)) => feed,
        Ok(None) => return not_found("no network feed found for fray"),
        Err(err) => return bad_gateway(err.to_string()),
    };
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => return bad_request("system clock before unix epoch".to_string()),
    };
    if !signed_feed.signed.verify() {
        warn!(fray = %fray, publisher = %signed_feed.signed.publisher_b64(), "rejected fray feed with invalid signature");
        return bad_gateway("fray feed signature verification failed".to_string());
    }
    if signed_feed.signed.signed_at > now.saturating_add(600) {
        warn!(fray = %fray, publisher = %signed_feed.signed.publisher_b64(), signed_at = signed_feed.signed.signed_at, now, "rejected fray feed signed too far in the future");
        return bad_gateway("fray feed signed_at is too far in the future".to_string());
    }
    let publisher_b64 = signed_feed.signed.publisher_b64();
    let trust_record = match network::fetch_trust_record(&fray, state.lattice_rpc_port).await {
        Ok(record) => record,
        Err(err) => return bad_gateway(err.to_string()),
    };
    if let Some(ref trust_record) = trust_record {
        if let Err(err) = network::import_trust_record(&state.store, trust_record) {
            return bad_request(err.to_string());
        }
    }
    let standings = network::standing_map(trust_record.as_ref());

    let mut imported_posts = 0usize;
    for mut post in signed_feed.feed.posts {
        if network::post_should_drop(&state.blocklist, &post) {
            continue;
        }
        post.publisher_key_b64 = Some(publisher_b64.clone());
        let action = match network::moderation_check_many(
            state.lattice_rpc_port,
            vec![
                ("PublisherKey".to_string(), publisher_b64.clone()),
                ("PostId".to_string(), post.id.clone()),
            ],
        )
        .await
        {
            Ok(action) => action,
            Err(err) => return bad_gateway(err.to_string()),
        };
        match action.as_deref() {
            Some("RejectIngest") | Some("Quarantine") => continue,
            Some("Hide") => post.hidden = true,
            _ => {}
        }
        if network::standing_hides_publisher(&standings, &publisher_b64) {
            post.hidden = true;
        }
        if state.store.upsert_post(post).is_ok() {
            imported_posts += 1;
        }
    }

    let mut imported_comments = 0usize;
    for mut comment in signed_feed.feed.comments {
        if network::comment_should_drop(&state.blocklist, &comment) {
            continue;
        }
        comment.publisher_key_b64 = Some(publisher_b64.clone());
        let action = match network::moderation_check_many(
            state.lattice_rpc_port,
            vec![
                ("PublisherKey".to_string(), publisher_b64.clone()),
                ("CommentId".to_string(), comment.id.clone()),
            ],
        )
        .await
        {
            Ok(action) => action,
            Err(err) => return bad_gateway(err.to_string()),
        };
        match action.as_deref() {
            Some("RejectIngest") | Some("Quarantine") => continue,
            Some("Hide") => comment.hidden = true,
            _ => {}
        }
        if network::standing_hides_publisher(&standings, &publisher_b64) {
            comment.hidden = true;
        }
        if state.store.upsert_comment(comment).is_ok() {
            imported_comments += 1;
        }
    }

    if let Err(err) = state.store.flush() {
        return bad_request(err.to_string());
    }
    Json(json!({
      "status":"ok",
      "fray": fray,
      "imported_posts": imported_posts,
      "imported_comments": imported_comments,
    }))
    .into_response()
}

async fn get_directory(State(state): State<AppState>) -> Response {
    match state.store.load_directory() {
        Ok(Some(directory)) => Json(directory).into_response(),
        Ok(None) => match network::fetch_directory(state.lattice_rpc_port).await {
            Ok(Some(directory)) => {
                let _ = state.store.store_directory(&directory);
                Json(directory).into_response()
            }
            Ok(None) => not_found("directory not found"),
            Err(err) => bad_gateway(err.to_string()),
        },
        Err(err) => bad_request(err.to_string()),
    }
}

async fn sync_directory(State(state): State<AppState>) -> Response {
    match network::fetch_directory(state.lattice_rpc_port).await {
        Ok(Some(directory)) => {
            if let Err(err) = state.store.store_directory(&directory) {
                return bad_request(err.to_string());
            }
            Json(json!({ "status": "ok" })).into_response()
        }
        Ok(None) => not_found("directory not found"),
        Err(err) => bad_gateway(err.to_string()),
    }
}

async fn upsert_directory_entry(State(state): State<AppState>, Json(request): Json<DirectoryEntryRequest>) -> Response {
    let mut signed = match state.store.load_directory() {
        Ok(Some(directory)) => directory,
        Ok(None) => {
            let local_key = signing_key_b64(state.signing_key.as_ref());
            SignedFrayDirectory {
                directory: FrayDirectory {
                    version: 1,
                    operator_key_b64: local_key,
                    entries: Vec::new(),
                    generated_at: unix_ts(),
                },
                signature_b64: String::new(),
            }
        }
        Err(err) => return bad_request(err.to_string()),
    };
    let local_key = signing_key_b64(state.signing_key.as_ref());
    if signed.directory.operator_key_b64 != local_key {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "local signing key is not the directory operator" })),
        )
            .into_response();
    }
    let status = match parse_directory_status(&request.status, None) {
        Ok(status) => status,
        Err(err) => return bad_request(err),
    };
    let now = unix_ts();
    let entry = FrayDirectoryEntry {
        fray_name: request.fray_name.clone(),
        owner_key_b64: request.owner_key_b64,
        status,
        listed_at: now,
        updated_at: now,
        description: request.description,
    };
    upsert_directory_entry_record(&mut signed.directory.entries, entry);
    signed.directory.generated_at = now;
    signed.directory.version = signed.directory.version.saturating_add(1);
    let signed = match network::publish_directory(
        &signed.directory,
        state.signing_key.as_ref(),
        state.lattice_rpc_port,
    )
    .await
    {
        Ok(()) => match crate::directory::sign_directory(&signed.directory, state.signing_key.as_ref()) {
            Ok(signed) => signed,
            Err(err) => return bad_request(err.to_string()),
        },
        Err(err) => return bad_gateway(err.to_string()),
    };
    if let Err(err) = state.store.store_directory(&signed) {
        return bad_request(err.to_string());
    }
    Json(json!({ "status": "ok" })).into_response()
}

async fn ban_directory_entry(
    State(state): State<AppState>,
    Path(fray_name): Path<String>,
) -> Response {
    let mut signed = match state.store.load_directory() {
        Ok(Some(directory)) => directory,
        Ok(None) => return not_found("directory not found"),
        Err(err) => return bad_request(err.to_string()),
    };
    let local_key = signing_key_b64(state.signing_key.as_ref());
    if signed.directory.operator_key_b64 != local_key {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "local signing key is not the directory operator" })),
        )
            .into_response();
    }
    let now = unix_ts();
    if let Some(entry) = signed
        .directory
        .entries
        .iter_mut()
        .find(|entry| entry.fray_name == fray_name)
    {
        entry.status = FrayStatus::Banned { reason: None };
        entry.updated_at = now;
    } else {
        signed.directory.entries.push(FrayDirectoryEntry {
            fray_name: fray_name.clone(),
            owner_key_b64: String::new(),
            status: FrayStatus::Banned { reason: None },
            listed_at: now,
            updated_at: now,
            description: None,
        });
    }
    signed.directory.generated_at = now;
    signed.directory.version = signed.directory.version.saturating_add(1);
    match network::publish_directory(
        &signed.directory,
        state.signing_key.as_ref(),
        state.lattice_rpc_port,
    )
    .await
    {
        Ok(()) => {
            let signed = match crate::directory::sign_directory(&signed.directory, state.signing_key.as_ref()) {
                Ok(signed) => signed,
                Err(err) => return bad_request(err.to_string()),
            };
            if let Err(err) = state.store.store_directory(&signed) {
                return bad_request(err.to_string());
            }
            Json(json!({ "status": "ok" })).into_response()
        }
        Err(err) => bad_gateway(err.to_string()),
    }
}

async fn add_blocklist_hash(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(err) = verify_body_signature(
        &headers,
        &body,
        &[signing_key_b64(state.signing_key.as_ref())],
    ) {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": err }))).into_response();
    }
    let request: BlocklistRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(err) => return bad_request(format!("invalid json: {err}")),
    };
    match state.blocklist.append_to_file(&state.blocklist_path, &request.hash_hex) {
        Ok(()) => Json(json!({ "status": "ok" })).into_response(),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn persist_trust_record(state: &AppState, fray: &str, record: &SignedTrustRecord) -> Response {
    if let Err(err) = state.store.store_trust_record(fray, record) {
        return bad_request(err.to_string());
    }
    for entry in &record.record.entries {
        if let Err(err) = state.store.store_key_record(fray, entry.clone()) {
            return bad_request(err.to_string());
        }
    }
    match network::publish_trust_record(
        fray,
        &record.record,
        state.signing_key.as_ref(),
        state.lattice_rpc_port,
    )
    .await
    {
        Ok(()) => Json(json!({ "status": "ok" })).into_response(),
        Err(err) => bad_gateway(err.to_string()),
    }
}

fn load_owned_trust_record(state: &AppState, fray: &str) -> Result<SignedTrustRecord, Response> {
    let Some(owner_key) = state
        .store
        .get_fray_ownership(fray)
        .map_err(|err| bad_request(err.to_string()))?
    else {
        return Err(not_found("fray is not owned locally"));
    };
    let local_key = signing_key_b64(state.signing_key.as_ref());
    if owner_key != local_key {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "local signing key does not own this fray" })),
        )
            .into_response());
    }
    let record = state
        .store
        .load_trust_record(fray)
        .map_err(|err| bad_request(err.to_string()))?
        .ok_or_else(|| not_found("trust record not found"))?;
    Ok(record)
}

fn moderator_authorized_keys(record: &FrayTrustRecord) -> Vec<String> {
    let mut out = vec![record.owner_key_b64.clone()];
    out.extend(record.moderator_keys.clone());
    out
}

fn verify_body_signature(headers: &HeaderMap, body: &[u8], allowed_keys: &[String]) -> Result<String, String> {
    let Some(signature_value) = headers.get("X-Fray-Signature") else {
        return Err("missing X-Fray-Signature header".to_string());
    };
    let signature_value = signature_value
        .to_str()
        .map_err(|_| "invalid X-Fray-Signature header".to_string())?;
    let signature_bytes = BASE64_STANDARD
        .decode(signature_value)
        .map_err(|_| "invalid signature base64".to_string())?;
    let signature = Signature::from_slice(&signature_bytes)
        .map_err(|_| "invalid signature bytes".to_string())?;

    for key_b64 in allowed_keys {
        let verifying_key = crate::trust::decode_public_key_b64(key_b64)?;
        if verifying_key.verify(body, &signature).is_ok() {
            return Ok(key_b64.clone());
        }
    }
    Err("signature did not verify against an authorized key".to_string())
}

fn signing_key_b64(signing_key: &SigningKey) -> String {
    BASE64_STANDARD.encode(signing_key.verifying_key().as_bytes())
}

fn parse_standing(value: &str, reason: Option<String>) -> Result<KeyStanding, String> {
    match value {
        "Trusted" => Ok(KeyStanding::Trusted),
        "Normal" => Ok(KeyStanding::Normal),
        "Restricted" => Ok(KeyStanding::Restricted { reason }),
        _ => Err("standing must be Trusted, Normal, or Restricted".to_string()),
    }
}

fn parse_directory_status(value: &str, reason: Option<String>) -> Result<FrayStatus, String> {
    match value {
        "Listed" => Ok(FrayStatus::Listed),
        "Unlisted" => Ok(FrayStatus::Unlisted),
        "Banned" => Ok(FrayStatus::Banned { reason }),
        _ => Err("status must be Listed, Unlisted, or Banned".to_string()),
    }
}

fn upsert_key_record(entries: &mut Vec<KeyRecord>, record: KeyRecord) {
    if let Some(existing) = entries.iter_mut().find(|entry| entry.key_b64 == record.key_b64) {
        *existing = record;
    } else {
        entries.push(record);
    }
    entries.sort_by(|a, b| a.key_b64.cmp(&b.key_b64));
}

fn upsert_directory_entry_record(entries: &mut Vec<FrayDirectoryEntry>, record: FrayDirectoryEntry) {
    if let Some(existing) = entries
        .iter_mut()
        .find(|entry| entry.fray_name == record.fray_name)
    {
        let listed_at = existing.listed_at;
        *existing = record;
        existing.listed_at = listed_at;
    } else {
        entries.push(record);
    }
    entries.sort_by(|a, b| a.fray_name.cmp(&b.fray_name));
}

fn standing_for_key(store: &FrayStore, fray: &str, key_b64: Option<&str>) -> Option<Value> {
    let key_b64 = key_b64?;
    let standing = store.get_key_standing(fray, key_b64).ok().flatten()?;
    let label = match &standing {
        KeyStanding::Trusted => "Trusted",
        KeyStanding::Normal => "Normal",
        KeyStanding::Restricted { .. } => "Restricted",
    };
    let reason = match &standing {
        KeyStanding::Restricted { reason } => reason.clone(),
        _ => None,
    };
    Some(json!({
        "standing": label,
        "reason": reason,
    }))
}

fn post_summary_response(store: &FrayStore, fray: &str, post: &Post) -> Value {
    let standing = standing_for_key(store, fray, post.publisher_key_b64.as_deref());
    json!({
        "id": post.id,
        "fray": post.fray,
        "author": post.author,
        "title": post.title,
        "created_at": post.created_at,
        "standing": standing.as_ref().and_then(|value| value.get("standing")).and_then(Value::as_str),
        "standing_reason": standing.as_ref().and_then(|value| value.get("reason")).and_then(Value::as_str),
    })
}

fn post_response(store: &FrayStore, fray: &str, post: &Post) -> Value {
    let standing = standing_for_key(store, fray, post.publisher_key_b64.as_deref());
    json!({
        "id": post.id,
        "fray": post.fray,
        "author": post.author,
        "title": post.title,
        "body": post.body,
        "created_at": post.created_at,
        "hidden": post.hidden,
        "publisher_key_b64": post.publisher_key_b64,
        "standing": standing.as_ref().and_then(|value| value.get("standing")).and_then(Value::as_str),
        "standing_reason": standing.as_ref().and_then(|value| value.get("reason")).and_then(Value::as_str),
    })
}

fn comment_response(store: &FrayStore, fray: &str, comment: &Comment) -> Value {
    let standing = standing_for_key(store, fray, comment.publisher_key_b64.as_deref());
    json!({
        "id": comment.id,
        "fray": comment.fray,
        "post_id": comment.post_id,
        "author": comment.author,
        "body": comment.body,
        "created_at": comment.created_at,
        "hidden": comment.hidden,
        "publisher_key_b64": comment.publisher_key_b64,
        "standing": standing.as_ref().and_then(|value| value.get("standing")).and_then(Value::as_str),
        "standing_reason": standing.as_ref().and_then(|value| value.get("reason")).and_then(Value::as_str),
    })
}

fn bad_request(message: String) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
}

fn bad_gateway(message: String) -> Response {
    (StatusCode::BAD_GATEWAY, Json(json!({ "error": message }))).into_response()
}

fn not_found(message: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    fn app_state() -> AppState {
        let path = std::env::temp_dir().join(format!("fray-api-test-{}", unix_ts()));
        let _ = std::fs::create_dir_all(&path);
        AppState {
            store: FrayStore::open(&path).expect("open store"),
            lattice_rpc_port: 9,
            signing_key: Arc::new(SigningKey::from_bytes(&[7; 32])),
            blocklist: ContentBlocklist::new(),
            blocklist_path: path.join("blocklist.txt"),
        }
    }

    #[tokio::test]
    async fn claim_returns_403_when_stake_check_fails() {
        let app = app(app_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/frays/lattice/claim")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({ "owner_key_b64": BASE64_STANDARD.encode(SigningKey::from_bytes(&[7; 32]).verifying_key().as_bytes()) }).to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
