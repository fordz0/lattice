use crate::blocklist::ContentBlocklist;
use crate::directory::{FrayDirectory, FrayDirectoryEntry, FrayStatus, SignedFrayDirectory};
use crate::handle::{validate_handle, validate_handle_record, FrayHandleRecord};
use crate::model::{Comment, CreateCommentRequest, CreatePostRequest, Post};
use crate::network;
use crate::store::FrayStore;
use crate::trust::{
    decode_public_key_b64, sign_trust_record, unix_ts, FrayTrustRecord, KeyRecord, KeyStanding,
    SignedTrustRecord,
};
use crate::ui;
use axum::body::to_bytes;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, Query, Request as AxumRequest, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use lattice_core::identity::canonical_json_bytes;
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
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

#[derive(Debug, Deserialize)]
struct ClaimHandleRequest {
    handle: String,
    display_name: Option<String>,
    bio: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct IdentityResponse {
    handle: Option<String>,
    display_name: Option<String>,
    bio: Option<String>,
    key_b64: String,
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
        .route("/api/v1/sign", post(sign_payload))
        .route("/api/v1/identity", get(get_local_identity))
        .route("/api/v1/identity/claim", post(claim_handle))
        .route("/api/v1/identity/transfer", post(transfer_handle))
        .route("/api/v1/identity/release", post(release_handle))
        .route("/api/v1/identity/{handle}", get(get_handle_identity))
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
        .route(
            "/api/v1/frays/{fray}/claim",
            get(get_fray_claim_status).post(claim_fray),
        )
        .route("/api/v1/frays/{fray}/trust", get(get_trust_record))
        .route(
            "/api/v1/frays/{fray}/trust/standings",
            post(set_trust_standing),
        )
        .route("/api/v1/frays/{fray}/trust/moderators", post(add_moderator))
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
        "POST /api/v1/sign",
        "GET /api/v1/identity",
        "POST /api/v1/identity/claim",
        "POST /api/v1/identity/transfer",
        "POST /api/v1/identity/release",
        "GET /api/v1/identity/{handle}",
        "GET /api/v1/directory",
        "POST /api/v1/directory/sync",
        "GET /api/v1/frays/{fray}/claim",
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

// /api/v1/sign is intentionally localhost-only. It signs arbitrary JSON with the
// node's private key so the browser UI can make authenticated admin requests without
// the key ever touching the browser. Since Fray binds to 127.0.0.1, this is only
// reachable from the local machine, but we add an explicit remote address check as
// defence in depth.
async fn sign_payload(State(state): State<AppState>, request: AxumRequest) -> Response {
    let remote_addr = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|value| value.0);
    if !remote_addr_is_loopback(remote_addr) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "sign endpoint is localhost-only" })),
        )
            .into_response();
    }
    let body = match to_bytes(request.into_body(), MAX_JSON_BODY_BYTES).await {
        Ok(body) => body,
        Err(err) => return bad_request(format!("failed to read request body: {err}")),
    };
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(err) => return bad_request(format!("invalid json: {err}")),
    };
    let payload = match canonical_json_bytes(&value) {
        Ok(payload) => payload,
        Err(err) => return bad_request(format!("failed to canonicalize json: {err}")),
    };
    let signature = state.signing_key.sign(&payload);
    Json(json!({
        "payload_b64": BASE64_STANDARD.encode(&payload),
        "signature_b64": BASE64_STANDARD.encode(signature.to_bytes()),
        "key_b64": signing_key_b64(state.signing_key.as_ref()),
    }))
    .into_response()
}

async fn get_local_identity(State(state): State<AppState>) -> Response {
    let key_b64 = signing_key_b64(state.signing_key.as_ref());
    let handle = match state.store.get_local_handle() {
        Ok(handle) => handle,
        Err(err) => return bad_request(err.to_string()),
    };
    let display_name = match state.store.get_local_display_name() {
        Ok(value) => value,
        Err(err) => return bad_request(err.to_string()),
    };
    let bio = match state.store.get_local_bio() {
        Ok(value) => value,
        Err(err) => return bad_request(err.to_string()),
    };
    Json(IdentityResponse {
        handle,
        display_name,
        bio,
        key_b64,
    })
    .into_response()
}

async fn get_handle_identity(
    State(state): State<AppState>,
    Path(handle): Path<String>,
) -> Response {
    match network::fetch_handle_record(state.lattice_rpc_port, &handle).await {
        Ok(Some(record)) if record.record.claimed_at != 0 => Json(json!({
            "handle": record.record.handle,
            "display_name": record.record.display_name,
            "bio": record.record.bio,
            "claimed_at": record.record.claimed_at,
            "previous_handle": record.record.previous_handle,
            "key_b64": record.signed.publisher_b64(),
        }))
        .into_response(),
        Ok(Some(_)) | Ok(None) => not_found("handle not found"),
        Err(err) => bad_gateway(err.to_string()),
    }
}

async fn claim_handle(
    State(state): State<AppState>,
    Json(request): Json<ClaimHandleRequest>,
) -> Response {
    let handle = request.handle.trim().to_lowercase();
    if let Err(err) = validate_handle(&handle) {
        return bad_request(err);
    }

    let existing_handle = match state.store.get_local_handle() {
        Ok(handle) => handle,
        Err(err) => return bad_request(err.to_string()),
    };
    if let Some(existing_handle) = existing_handle.as_ref() {
        if existing_handle != &handle {
            return bad_request(
                "handle cannot be changed via this endpoint — use the transfer flow".to_string(),
            );
        }
    }

    let record = match publish_handle_claim(&state, &handle, &request, None).await {
        Ok(record) => record,
        Err(response) => return response,
    };
    if let Err(response) = persist_local_identity(&state.store, &record) {
        return response;
    }

    Json(json!({ "handle": handle, "claimed": true })).into_response()
}

async fn transfer_handle(
    State(state): State<AppState>,
    Json(request): Json<ClaimHandleRequest>,
) -> Response {
    let new_handle = request.handle.trim().to_lowercase();
    if let Err(err) = validate_handle(&new_handle) {
        return bad_request(err);
    }

    let Some(old_handle) = (match state.store.get_local_handle() {
        Ok(handle) => handle,
        Err(err) => return bad_request(err.to_string()),
    }) else {
        return not_found("no local handle claimed");
    };

    if old_handle == new_handle {
        return bad_request("new handle must differ from current handle".to_string());
    }

    let record =
        match publish_handle_claim(&state, &new_handle, &request, Some(old_handle.clone())).await {
            Ok(record) => record,
            Err(response) => return response,
        };
    if let Err(response) = tombstone_handle(&state, &old_handle).await {
        return response;
    }
    if let Err(response) = persist_local_identity(&state.store, &record) {
        return response;
    }

    Json(json!({ "handle": new_handle, "claimed": true, "transferred": true })).into_response()
}

async fn release_handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let local_key = signing_key_b64(state.signing_key.as_ref());
    if let Err(err) = verify_body_signature(&headers, &body, std::slice::from_ref(&local_key)) {
        return (StatusCode::FORBIDDEN, Json(json!({ "error": err }))).into_response();
    }
    let Some(handle) = (match state.store.get_local_handle() {
        Ok(handle) => handle,
        Err(err) => return bad_request(err.to_string()),
    }) else {
        return not_found("no local handle claimed");
    };
    let tombstone = FrayHandleRecord {
        handle: handle.clone(),
        display_name: None,
        bio: None,
        claimed_at: 0,
        previous_handle: None,
    };
    if let Err(err) = network::publish_handle_record(
        &handle,
        &tombstone,
        state.signing_key.as_ref(),
        state.lattice_rpc_port,
    )
    .await
    {
        return bad_gateway(err.to_string());
    }
    if let Err(err) = state.store.clear_local_identity() {
        return bad_request(err.to_string());
    }
    Json(json!({ "released": true, "handle": handle })).into_response()
}

async fn create_post(
    State(state): State<AppState>,
    Path(fray): Path<String>,
    Json(request): Json<CreatePostRequest>,
) -> Response {
    let author = match resolved_author(&state) {
        Ok(author) => author,
        Err(err) => return bad_request(err.to_string()),
    };
    match state.store.create_post(&fray, &author, request) {
        Ok(mut post) => {
            apply_post_signature(&state, &mut post);
            if let Err(err) = state.store.upsert_post(post.clone()) {
                return bad_request(err.to_string());
            }
            let response = post_response(&state, &fray, &post).await;
            (StatusCode::CREATED, Json(response)).into_response()
        }
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
            let mut out = Vec::with_capacity(posts.len());
            for post in &posts {
                out.push(post_summary_response(&state, &fray, post).await);
            }
            Json(json!({ "posts": out })).into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn get_post(
    State(state): State<AppState>,
    Path((fray, post_id)): Path<(String, String)>,
) -> Response {
    match state.store.get_post(&fray, &post_id) {
        Ok(Some(post)) => Json(post_response(&state, &fray, &post).await).into_response(),
        Ok(None) => not_found("post not found"),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn create_comment(
    State(state): State<AppState>,
    Path((fray, post_id)): Path<(String, String)>,
    Json(request): Json<CreateCommentRequest>,
) -> Response {
    let author = match resolved_author(&state) {
        Ok(author) => author,
        Err(err) => return bad_request(err.to_string()),
    };
    match state
        .store
        .create_comment(&fray, &post_id, &author, request)
    {
        Ok(mut comment) => {
            apply_comment_signature(&state, &mut comment);
            if let Err(err) = state.store.upsert_comment(comment.clone()) {
                return bad_request(err.to_string());
            }
            (
                StatusCode::CREATED,
                Json(comment_response(&state, &fray, &comment).await),
            )
                .into_response()
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
            let mut out = Vec::with_capacity(comments.len());
            for comment in &comments {
                out.push(comment_response(&state, &fray, comment).await);
            }
            Json(json!({ "comments": out })).into_response()
        }
        Err(err) => bad_request(err.to_string()),
    }
}

async fn claim_fray(State(state): State<AppState>, Path(fray): Path<String>) -> Response {
    match network::check_frayloom_stake(state.lattice_rpc_port).await {
        Ok(true) => {}
        Ok(false) => return (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "you must have fray.loom pinned and trusted to claim a fray" })),
        )
            .into_response(),
        Err(err) => return bad_gateway(err.to_string()),
    }

    let local_key = signing_key_b64(state.signing_key.as_ref());
    if let Ok(Some(existing)) = state.store.get_fray_ownership(&fray) {
        if existing != local_key {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": "fray is already claimed" })),
            )
                .into_response();
        }
    }

    let trust_record = FrayTrustRecord {
        version: 1,
        fray: fray.clone(),
        owner_key_b64: local_key.clone(),
        moderator_keys: Vec::new(),
        entries: Vec::new(),
        generated_at: unix_ts(),
    };
    let signed = match sign_trust_record(&trust_record, state.signing_key.as_ref()) {
        Ok(record) => record,
        Err(err) => return bad_request(err.to_string()),
    };
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
    if let Err(err) = state.store.store_trust_record(&fray, &signed) {
        return bad_request(err.to_string());
    }
    if let Err(err) = state.store.store_fray_ownership(&fray, &local_key) {
        return bad_request(err.to_string());
    }

    Json(json!({ "status": "ok", "fray": fray, "owner_key_b64": local_key })).into_response()
}

async fn get_fray_claim_status(
    State(state): State<AppState>,
    Path(fray): Path<String>,
) -> Response {
    if let Some(owner_key_b64) = match state.store.get_fray_ownership(&fray) {
        Ok(owner_key_b64) => owner_key_b64,
        Err(err) => return bad_request(err.to_string()),
    } {
        return Json(json!({
            "claimed": true,
            "owner_key_b64": owner_key_b64,
            "local": true,
        }))
        .into_response();
    };

    match network::fetch_trust_record(&fray, state.lattice_rpc_port).await {
        Ok(Some(record)) => Json(json!({
            "claimed": true,
            "owner_key_b64": record.record.record.owner_key_b64,
            "local": false,
        }))
        .into_response(),
        Ok(None) => Json(json!({ "claimed": false })).into_response(),
        Err(err) => bad_gateway(err.to_string()),
    }
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
    record
        .record
        .moderator_keys
        .retain(|value| value != &key_b64);
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
        if let Err(err) = network::import_trust_record(&state.store, &trust_record.record) {
            return bad_request(err.to_string());
        }
    }
    let standings = network::standing_map(trust_record.as_ref().map(|record| &record.record));

    let mut imported_posts = 0usize;
    for mut post in signed_feed.feed.posts {
        if network::post_should_drop(&state.blocklist, &post) {
            continue;
        }
        if post.key_b64.is_none() {
            post.key_b64 = Some(publisher_b64.clone());
        }
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
        if comment.key_b64.is_none() {
            comment.key_b64 = Some(publisher_b64.clone());
        }
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

async fn upsert_directory_entry(
    State(state): State<AppState>,
    Json(request): Json<DirectoryEntryRequest>,
) -> Response {
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
        Ok(()) => {
            match crate::directory::sign_directory(&signed.directory, state.signing_key.as_ref()) {
                Ok(signed) => signed,
                Err(err) => return bad_request(err.to_string()),
            }
        }
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
            let signed = match crate::directory::sign_directory(
                &signed.directory,
                state.signing_key.as_ref(),
            ) {
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
    match state
        .blocklist
        .append_to_file(&state.blocklist_path, &request.hash_hex)
    {
        Ok(()) => Json(json!({ "status": "ok" })).into_response(),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn persist_trust_record(
    state: &AppState,
    fray: &str,
    record: &SignedTrustRecord,
) -> Response {
    match network::publish_trust_record(
        fray,
        &record.record,
        state.signing_key.as_ref(),
        state.lattice_rpc_port,
    )
    .await
    {
        Ok(()) => {}
        Err(err) => return bad_gateway(err.to_string()),
    }
    if let Err(err) = state.store.store_trust_record(fray, record) {
        return bad_request(err.to_string());
    }
    for entry in &record.record.entries {
        if let Err(err) = state.store.store_key_record(fray, entry.clone()) {
            return bad_request(err.to_string());
        }
    }
    Json(json!({ "status": "ok" })).into_response()
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

fn verify_body_signature(
    headers: &HeaderMap,
    body: &[u8],
    allowed_keys: &[String],
) -> Result<String, String> {
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

    let canonical_payload = serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| canonical_json_bytes(&value).ok());

    for key_b64 in allowed_keys {
        let verifying_key = crate::trust::decode_public_key_b64(key_b64)?;
        if verifying_key.verify(body, &signature).is_ok() {
            return Ok(key_b64.clone());
        }
        if let Some(payload) = canonical_payload.as_ref() {
            if verifying_key.verify(payload, &signature).is_ok() {
                return Ok(key_b64.clone());
            }
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
    if let Some(existing) = entries
        .iter_mut()
        .find(|entry| entry.key_b64 == record.key_b64)
    {
        *existing = record;
    } else {
        entries.push(record);
    }
    entries.sort_by(|a, b| a.key_b64.cmp(&b.key_b64));
}

fn upsert_directory_entry_record(
    entries: &mut Vec<FrayDirectoryEntry>,
    record: FrayDirectoryEntry,
) {
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

async fn post_summary_response(state: &AppState, fray: &str, post: &Post) -> Value {
    let standing = standing_for_key(&state.store, fray, post.key_b64.as_deref());
    let verified = verify_post_identity(state, post).await;
    json!({
        "id": post.id,
        "fray": post.fray,
        "author": post.author,
        "title": post.title,
        "created_at": post.created_at,
        "key_b64": post.key_b64,
        "signature_b64": post.signature_b64,
        "verified": verified,
        "standing": standing.as_ref().and_then(|value| value.get("standing")).and_then(Value::as_str),
        "standing_reason": standing.as_ref().and_then(|value| value.get("reason")).and_then(Value::as_str),
    })
}

async fn post_response(state: &AppState, fray: &str, post: &Post) -> Value {
    let standing = standing_for_key(&state.store, fray, post.key_b64.as_deref());
    let verified = verify_post_identity(state, post).await;
    json!({
        "id": post.id,
        "fray": post.fray,
        "author": post.author,
        "title": post.title,
        "body": post.body,
        "created_at": post.created_at,
        "hidden": post.hidden,
        "key_b64": post.key_b64,
        "signature_b64": post.signature_b64,
        "verified": verified,
        "standing": standing.as_ref().and_then(|value| value.get("standing")).and_then(Value::as_str),
        "standing_reason": standing.as_ref().and_then(|value| value.get("reason")).and_then(Value::as_str),
    })
}

async fn comment_response(state: &AppState, fray: &str, comment: &Comment) -> Value {
    let standing = standing_for_key(&state.store, fray, comment.key_b64.as_deref());
    let verified = verify_comment_identity(state, comment).await;
    json!({
        "id": comment.id,
        "fray": comment.fray,
        "post_id": comment.post_id,
        "author": comment.author,
        "body": comment.body,
        "created_at": comment.created_at,
        "hidden": comment.hidden,
        "key_b64": comment.key_b64,
        "signature_b64": comment.signature_b64,
        "verified": verified,
        "standing": standing.as_ref().and_then(|value| value.get("standing")).and_then(Value::as_str),
        "standing_reason": standing.as_ref().and_then(|value| value.get("reason")).and_then(Value::as_str),
    })
}

fn resolved_author(state: &AppState) -> Result<String, anyhow::Error> {
    if let Some(handle) = state.store.get_local_handle()? {
        return Ok(handle);
    }
    Ok("anonymous".to_string())
}

#[derive(serde::Serialize)]
struct PostSignaturePayload<'a> {
    fray: &'a str,
    author: &'a str,
    title: &'a str,
    body: &'a str,
    created_at: u64,
}

#[derive(serde::Serialize)]
struct CommentSignaturePayload<'a> {
    fray: &'a str,
    post_id: &'a str,
    author: &'a str,
    body: &'a str,
    created_at: u64,
}

enum AuthorshipPayload<'a> {
    Post(PostSignaturePayload<'a>),
    Comment(CommentSignaturePayload<'a>),
}

fn apply_post_signature(state: &AppState, post: &mut Post) {
    if let Ok(payload) = canonical_json_bytes(&PostSignaturePayload {
        fray: &post.fray,
        author: &post.author,
        title: &post.title,
        body: &post.body,
        created_at: post.created_at,
    }) {
        let signature = state.signing_key.sign(&payload);
        post.key_b64 = Some(signing_key_b64(state.signing_key.as_ref()));
        post.signature_b64 = Some(BASE64_STANDARD.encode(signature.to_bytes()));
    }
}

fn apply_comment_signature(state: &AppState, comment: &mut Comment) {
    if let Ok(payload) = canonical_json_bytes(&CommentSignaturePayload {
        fray: &comment.fray,
        post_id: &comment.post_id,
        author: &comment.author,
        body: &comment.body,
        created_at: comment.created_at,
    }) {
        let signature = state.signing_key.sign(&payload);
        comment.key_b64 = Some(signing_key_b64(state.signing_key.as_ref()));
        comment.signature_b64 = Some(BASE64_STANDARD.encode(signature.to_bytes()));
    }
}

async fn verify_post_identity(state: &AppState, post: &Post) -> bool {
    verify_authorship(
        state,
        &post.author,
        post.key_b64.as_deref(),
        post.signature_b64.as_deref(),
        AuthorshipPayload::Post(PostSignaturePayload {
            fray: &post.fray,
            author: &post.author,
            title: &post.title,
            body: &post.body,
            created_at: post.created_at,
        }),
    )
    .await
}

async fn verify_comment_identity(state: &AppState, comment: &Comment) -> bool {
    verify_authorship(
        state,
        &comment.author,
        comment.key_b64.as_deref(),
        comment.signature_b64.as_deref(),
        AuthorshipPayload::Comment(CommentSignaturePayload {
            fray: &comment.fray,
            post_id: &comment.post_id,
            author: &comment.author,
            body: &comment.body,
            created_at: comment.created_at,
        }),
    )
    .await
}

async fn verify_authorship(
    state: &AppState,
    author: &str,
    key_b64: Option<&str>,
    signature_b64: Option<&str>,
    payload: AuthorshipPayload<'_>,
) -> bool {
    let (Some(key_b64), Some(signature_b64)) = (key_b64, signature_b64) else {
        return false;
    };
    let verifying_key = match decode_public_key_b64(key_b64) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let signature_bytes = match BASE64_STANDARD.decode(signature_b64) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };
    let signature = match Signature::from_slice(&signature_bytes) {
        Ok(signature) => signature,
        Err(_) => return false,
    };
    let payload = match payload {
        AuthorshipPayload::Post(payload) => match canonical_json_bytes(&payload) {
            Ok(payload) => payload,
            Err(_) => return false,
        },
        AuthorshipPayload::Comment(payload) => match canonical_json_bytes(&payload) {
            Ok(payload) => payload,
            Err(_) => return false,
        },
    };
    if verifying_key.verify(&payload, &signature).is_err() {
        return false;
    }
    if validate_handle(author).is_err() {
        return false;
    }
    matches!(
        network::publisher_owns_handle(state.lattice_rpc_port, author, key_b64).await,
        Ok(true)
    )
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

fn remote_addr_is_loopback(remote_addr: Option<SocketAddr>) -> bool {
    remote_addr
        .map(|addr| addr.ip().is_loopback())
        .unwrap_or(false)
}

async fn publish_handle_claim(
    state: &AppState,
    handle: &str,
    request: &ClaimHandleRequest,
    previous_handle: Option<String>,
) -> Result<FrayHandleRecord, Response> {
    let local_key_b64 = signing_key_b64(state.signing_key.as_ref());
    match network::fetch_handle_record(state.lattice_rpc_port, handle).await {
        Ok(Some(existing))
            if existing.record.claimed_at != 0
                && existing.signed.publisher_b64() != local_key_b64 =>
        {
            return Err((
                StatusCode::CONFLICT,
                Json(json!({ "error": "handle already claimed by another key" })),
            )
                .into_response());
        }
        Ok(_) => {}
        Err(err) => return Err(bad_gateway(err.to_string())),
    }

    let record = FrayHandleRecord {
        handle: handle.to_string(),
        display_name: request
            .display_name
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        bio: request
            .bio
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        claimed_at: unix_ts(),
        previous_handle,
    };
    if let Err(err) = validate_handle_record(&record) {
        return Err(bad_request(err));
    }
    if let Err(err) = network::publish_handle_record(
        handle,
        &record,
        state.signing_key.as_ref(),
        state.lattice_rpc_port,
    )
    .await
    {
        return Err(map_handle_publish_error(err));
    }
    Ok(record)
}

async fn tombstone_handle(state: &AppState, handle: &str) -> Result<(), Response> {
    let tombstone = FrayHandleRecord {
        handle: handle.to_string(),
        display_name: None,
        bio: None,
        claimed_at: 0,
        previous_handle: None,
    };
    if let Err(err) = network::publish_handle_record(
        handle,
        &tombstone,
        state.signing_key.as_ref(),
        state.lattice_rpc_port,
    )
    .await
    {
        return Err(bad_gateway(err.to_string()));
    }
    Ok(())
}

fn persist_local_identity(store: &FrayStore, record: &FrayHandleRecord) -> Result<(), Response> {
    if let Err(err) = store.set_local_handle(&record.handle) {
        return Err(bad_request(err.to_string()));
    }
    if let Some(display_name) = &record.display_name {
        if let Err(err) = store.set_local_display_name(display_name) {
            return Err(bad_request(err.to_string()));
        }
    } else if let Err(err) = store.set_local_display_name("") {
        return Err(bad_request(err.to_string()));
    }
    if let Some(bio) = &record.bio {
        if let Err(err) = store.set_local_bio(bio) {
            return Err(bad_request(err.to_string()));
        }
    } else if let Err(err) = store.set_local_bio("") {
        return Err(bad_request(err.to_string()));
    }
    Ok(())
}

fn map_handle_publish_error(err: anyhow::Error) -> Response {
    if err
        .to_string()
        .contains("app record owned by a different key")
    {
        (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "handle already claimed by another key" })),
        )
            .into_response()
    } else {
        bad_gateway(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::body::Body;
    use axum::extract::State as ExtractState;
    use axum::http::Request;
    use axum::routing::post as route_post;
    use axum::{Json as AxumJson, Router as AxumRouter};
    use std::collections::HashMap;
    use tokio::sync::{oneshot, Mutex};
    use tower::util::ServiceExt;

    fn temp_db_path() -> std::path::PathBuf {
        let mut random = [0_u8; 4];
        let _ = getrandom::getrandom(&mut random);
        std::env::temp_dir().join(format!("fray-api-test-{}", hex::encode(random)))
    }

    fn app_state_with_rpc_port(rpc_port: u16) -> AppState {
        let path = temp_db_path();
        let _ = std::fs::create_dir_all(&path);
        AppState {
            store: FrayStore::open(&path).expect("open store"),
            lattice_rpc_port: rpc_port,
            signing_key: Arc::new(SigningKey::from_bytes(&[7; 32])),
            blocklist: ContentBlocklist::new(),
            blocklist_path: path.join("blocklist.txt"),
        }
    }

    fn app_state() -> AppState {
        app_state_with_rpc_port(9)
    }

    fn decode_signature(signature_b64: &str) -> Signature {
        let bytes = BASE64_STANDARD
            .decode(signature_b64)
            .expect("decode signature");
        Signature::from_slice(&bytes).expect("parse signature")
    }

    async fn spawn_claim_rpc_server() -> (u16, oneshot::Sender<()>) {
        let records = Arc::new(Mutex::new(HashMap::<String, String>::new()));
        let app = AxumRouter::new()
            .route("/", route_post(mock_claim_rpc))
            .with_state(records);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let port = listener.local_addr().expect("local addr").port();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve mock rpc");
        });
        (port, shutdown_tx)
    }

    async fn mock_claim_rpc(
        ExtractState(records): ExtractState<Arc<Mutex<HashMap<String, String>>>>,
        AxumJson(request): AxumJson<Value>,
    ) -> AxumJson<Value> {
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = request.get("params").cloned().unwrap_or(Value::Null);
        let result = match method {
            "known_publisher_status" => json!({
                "explicitly_trusted": true,
            }),
            "get_site_manifest" => json!({
                "trust": {
                    "status": "matches",
                    "explicitly_trusted": true,
                },
                "pinned": true,
            }),
            "put_record" => {
                let key = params
                    .get("key")
                    .and_then(Value::as_str)
                    .expect("put_record key");
                let value = params
                    .get("value")
                    .and_then(Value::as_str)
                    .expect("put_record value");
                records
                    .lock()
                    .await
                    .insert(key.to_string(), value.to_string());
                json!({ "status": "ok" })
            }
            "get_record" => {
                let key = params
                    .get("key")
                    .and_then(Value::as_str)
                    .expect("get_record key");
                records
                    .lock()
                    .await
                    .get(key)
                    .cloned()
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            }
            _ => Value::Null,
        };
        AxumJson(json!({
            "jsonrpc": "2.0",
            "id": request.get("id").cloned().unwrap_or(json!(1)),
            "result": result,
        }))
    }

    #[tokio::test]
    async fn claim_returns_403_when_stake_check_fails() {
        let app = app(app_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/frays/lattice/claim")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn get_claim_status_returns_false_for_unclaimed_fray() {
        let (rpc_port, shutdown_tx) = spawn_claim_rpc_server().await;
        let app = app(app_state_with_rpc_port(rpc_port));
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/frays/lattice/claim")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let value: Value = serde_json::from_slice(&body).expect("decode response");
        assert_eq!(value, json!({ "claimed": false }));
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn get_claim_status_returns_remote_owner_from_dht() {
        let (rpc_port, shutdown_tx) = spawn_claim_rpc_server().await;
        let state = app_state_with_rpc_port(rpc_port);
        let owner = SigningKey::from_bytes(&[5; 32]);
        let owner_key_b64 = BASE64_STANDARD.encode(owner.verifying_key().as_bytes());
        let trust_record = FrayTrustRecord {
            version: 1,
            fray: "lattice".to_string(),
            owner_key_b64: owner_key_b64.clone(),
            moderator_keys: Vec::new(),
            entries: Vec::new(),
            generated_at: unix_ts(),
        };
        network::publish_trust_record("lattice", &trust_record, &owner, rpc_port)
            .await
            .expect("publish trust record");

        let app = app(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/frays/lattice/claim")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let value: Value = serde_json::from_slice(&body).expect("decode response");
        assert_eq!(
            value,
            json!({
                "claimed": true,
                "owner_key_b64": owner_key_b64,
                "local": false,
            })
        );
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn claim_succeeds_without_request_body() {
        let (rpc_port, shutdown_tx) = spawn_claim_rpc_server().await;
        let state = app_state_with_rpc_port(rpc_port);
        let expected_owner = signing_key_b64(state.signing_key.as_ref());
        let app = app(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/frays/lattice/claim")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            state
                .store
                .get_fray_ownership("lattice")
                .expect("get ownership"),
            Some(expected_owner.clone())
        );
        let trust = state
            .store
            .load_trust_record("lattice")
            .expect("load trust record")
            .expect("stored trust record");
        assert_eq!(trust.record.owner_key_b64, expected_owner);
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn claim_handle_rejects_handle_change_when_handle_is_already_set() {
        let state = app_state();
        state
            .store
            .set_local_handle("fordz0")
            .expect("set local handle");
        let app = app(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/identity/claim")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "handle": "newhandle",
                            "display_name": "Ford",
                            "bio": "hello"
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            state.store.get_local_handle().expect("get handle"),
            Some("fordz0".to_string())
        );
    }

    #[tokio::test]
    async fn transfer_handle_leaves_old_handle_intact_if_new_claim_fails() {
        let state = app_state_with_rpc_port(9);
        state
            .store
            .set_local_handle("fordz0")
            .expect("set local handle");
        let app = app(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/identity/transfer")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "handle": "newhandle",
                            "display_name": "Ford",
                            "bio": "hello"
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let value: Value = serde_json::from_slice(&body).expect("decode response");
        assert!(value
            .get("error")
            .and_then(Value::as_str)
            .expect("error string")
            .contains("failed to reach lattice daemon RPC"));
        assert_eq!(
            state.store.get_local_handle().expect("get handle"),
            Some("fordz0".to_string())
        );
    }

    #[tokio::test]
    async fn sign_endpoint_rejects_requests_without_loopback_remote_addr() {
        let app = app(app_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/sign")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "ok": true }).to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn post_signatures_bind_fray_and_author() {
        let state = app_state();
        let mut post = Post {
            id: "abc123-abcdef".to_string(),
            fray: "lattice".to_string(),
            author: "fordz0".to_string(),
            title: "hello".to_string(),
            body: "world".to_string(),
            created_at: unix_ts(),
            key_b64: None,
            signature_b64: None,
            hidden: false,
        };
        apply_post_signature(&state, &mut post);

        let signature = decode_signature(post.signature_b64.as_deref().expect("signature"));
        let verifying_key = state.signing_key.verifying_key();
        let exact_payload = canonical_json_bytes(&PostSignaturePayload {
            fray: &post.fray,
            author: &post.author,
            title: &post.title,
            body: &post.body,
            created_at: post.created_at,
        })
        .expect("encode exact payload");
        verifying_key
            .verify(&exact_payload, &signature)
            .expect("verify exact payload");

        let wrong_author_payload = canonical_json_bytes(&PostSignaturePayload {
            fray: &post.fray,
            author: "someoneelse",
            title: &post.title,
            body: &post.body,
            created_at: post.created_at,
        })
        .expect("encode wrong author payload");
        assert!(verifying_key
            .verify(&wrong_author_payload, &signature)
            .is_err());

        let wrong_fray_payload = canonical_json_bytes(&PostSignaturePayload {
            fray: "otherfray",
            author: &post.author,
            title: &post.title,
            body: &post.body,
            created_at: post.created_at,
        })
        .expect("encode wrong fray payload");
        assert!(verifying_key
            .verify(&wrong_fray_payload, &signature)
            .is_err());
    }

    #[test]
    fn comment_signatures_bind_fray_post_id_and_author() {
        let state = app_state();
        let mut comment = Comment {
            id: "abc123-fedcba".to_string(),
            fray: "lattice".to_string(),
            post_id: "post-123".to_string(),
            author: "fordz0".to_string(),
            body: "hello".to_string(),
            created_at: unix_ts(),
            key_b64: None,
            signature_b64: None,
            hidden: false,
        };
        apply_comment_signature(&state, &mut comment);

        let signature = decode_signature(comment.signature_b64.as_deref().expect("signature"));
        let verifying_key = state.signing_key.verifying_key();
        let exact_payload = canonical_json_bytes(&CommentSignaturePayload {
            fray: &comment.fray,
            post_id: &comment.post_id,
            author: &comment.author,
            body: &comment.body,
            created_at: comment.created_at,
        })
        .expect("encode exact payload");
        verifying_key
            .verify(&exact_payload, &signature)
            .expect("verify exact payload");

        let wrong_context_payload = canonical_json_bytes(&CommentSignaturePayload {
            fray: "otherfray",
            post_id: &comment.post_id,
            author: &comment.author,
            body: &comment.body,
            created_at: comment.created_at,
        })
        .expect("encode wrong fray payload");
        assert!(verifying_key
            .verify(&wrong_context_payload, &signature)
            .is_err());

        let wrong_post_payload = canonical_json_bytes(&CommentSignaturePayload {
            fray: &comment.fray,
            post_id: "post-999",
            author: &comment.author,
            body: &comment.body,
            created_at: comment.created_at,
        })
        .expect("encode wrong post payload");
        assert!(verifying_key
            .verify(&wrong_post_payload, &signature)
            .is_err());
    }

    #[test]
    fn comment_signature_from_one_fray_does_not_verify_in_another_context() {
        let state = app_state();
        let mut comment = Comment {
            id: "abc123-999999".to_string(),
            fray: "lattice".to_string(),
            post_id: "post-123".to_string(),
            author: "fordz0".to_string(),
            body: "hello".to_string(),
            created_at: unix_ts(),
            key_b64: None,
            signature_b64: None,
            hidden: false,
        };
        apply_comment_signature(&state, &mut comment);

        let signature = decode_signature(comment.signature_b64.as_deref().expect("signature"));
        let verifying_key = state.signing_key.verifying_key();
        let transplanted_payload = canonical_json_bytes(&CommentSignaturePayload {
            fray: "garden",
            post_id: &comment.post_id,
            author: &comment.author,
            body: &comment.body,
            created_at: comment.created_at,
        })
        .expect("encode transplanted payload");
        assert!(verifying_key
            .verify(&transplanted_payload, &signature)
            .is_err());
    }
}
