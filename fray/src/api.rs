use crate::model::{CreateCommentRequest, CreatePostRequest};
use crate::network;
use crate::store::FrayStore;
use crate::ui;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use ed25519_dalek::SigningKey;
use serde::Deserialize;
use serde_json::json;
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
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub limit: Option<usize>,
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui_index))
        .route("/f/{fray}", get(ui_index))
        .route("/f/{fray}/{post_id}", get(ui_index))
        .route("/health", get(health))
        .route("/api/v1/info", get(info))
        .route(
            "/api/v1/frays/{fray}/posts",
            get(list_posts).post(create_post),
        )
        .route("/api/v1/frays/{fray}/posts/{post_id}", get(get_post))
        .route(
            "/api/v1/frays/{fray}/posts/{post_id}/comments",
            get(list_comments).post(create_comment),
        )
        .route("/api/v1/frays/{fray}/sync/publish", post(publish_fray))
        .route("/api/v1/frays/{fray}/sync/pull", post(pull_fray))
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
      "ui_routes": ["/", "/f/{fray}", "/f/{fray}/{post_id}"],
      "routes": [
        "GET /api/v1/info",
        "GET /api/v1/frays/{fray}/posts",
        "POST /api/v1/frays/{fray}/posts",
        "GET /api/v1/frays/{fray}/posts/{post_id}",
        "GET /api/v1/frays/{fray}/posts/{post_id}/comments",
        "POST /api/v1/frays/{fray}/posts/{post_id}/comments",
        "POST /api/v1/frays/{fray}/sync/publish",
        "POST /api/v1/frays/{fray}/sync/pull"
      ]
    }))
}

async fn create_post(
    State(state): State<AppState>,
    Path(fray): Path<String>,
    Json(request): Json<CreatePostRequest>,
) -> Response {
    match state.store.create_post(&fray, request) {
        Ok(post) => (StatusCode::CREATED, Json(post)).into_response(),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn list_posts(
    State(state): State<AppState>,
    Path(fray): Path<String>,
    Query(query): Query<ListQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(50);
    match state.store.list_posts(&fray, limit) {
        Ok(posts) => Json(json!({ "posts": posts })).into_response(),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn get_post(
    State(state): State<AppState>,
    Path((fray, post_id)): Path<(String, String)>,
) -> Response {
    match state.store.get_post(&fray, &post_id) {
        Ok(Some(post)) => Json(post).into_response(),
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
        Ok(comment) => (StatusCode::CREATED, Json(comment)).into_response(),
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
    match state.store.list_comments(&fray, &post_id, limit) {
        Ok(comments) => Json(json!({ "comments": comments })).into_response(),
        Err(err) => bad_request(err.to_string()),
    }
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
    let feed = signed_feed.feed;
    let publisher_b64 = signed_feed.signed.publisher_b64();

    let mut imported_posts = 0usize;
    for mut post in feed.posts {
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
        if state.store.upsert_post(post).is_ok() {
            imported_posts += 1;
        }
    }

    let mut imported_comments = 0usize;
    for mut comment in feed.comments {
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

fn bad_request(message: String) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
}

fn bad_gateway(message: String) -> Response {
    (StatusCode::BAD_GATEWAY, Json(json!({ "error": message }))).into_response()
}

fn not_found(message: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": message }))).into_response()
}
