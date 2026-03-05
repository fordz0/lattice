use crate::model::CreatePostRequest;
use crate::network;
use crate::store::FrayStore;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct AppState {
    pub store: FrayStore,
    pub lattice_rpc_port: u16,
}

#[derive(Debug, Deserialize)]
pub struct ListPostsQuery {
    pub limit: Option<usize>,
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/info", get(info))
        .route("/api/f/{fray}/posts", get(list_posts).post(create_post))
        .route("/api/f/{fray}/posts/{post_id}", get(get_post))
        .route(
            "/api/v1/frays/{fray}/posts",
            get(list_posts).post(create_post),
        )
        .route("/api/v1/frays/{fray}/posts/{post_id}", get(get_post))
        .route(
            "/api/v1/frays/{fray}/sync/publish",
            axum::routing::post(publish_fray),
        )
        .route(
            "/api/v1/frays/{fray}/sync/pull",
            axum::routing::post(pull_fray),
        )
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({"status":"ok"}))
}

async fn info(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
      "service": "fray",
      "api_version": "v1",
      "lattice_rpc_port": state.lattice_rpc_port,
      "routes": [
        "GET /api/v1/info",
        "GET /api/v1/frays/{fray}/posts",
        "POST /api/v1/frays/{fray}/posts",
        "GET /api/v1/frays/{fray}/posts/{post_id}",
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
    Query(query): Query<ListPostsQuery>,
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
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"post not found"})),
        )
            .into_response(),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn publish_fray(State(state): State<AppState>, Path(fray): Path<String>) -> Response {
    let posts = match state.store.list_posts_full(&fray, 200) {
        Ok(posts) => posts,
        Err(err) => return bad_request(err.to_string()),
    };
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => return bad_request("system clock before unix epoch".to_string()),
    };

    match network::publish_feed(state.lattice_rpc_port, &fray, posts.clone(), now).await {
        Ok(()) => Json(json!({
            "status": "ok",
            "fray": fray,
            "published_posts": posts.len(),
            "record_key": format!("app:fray:feed:{}", fray),
        }))
        .into_response(),
        Err(err) => bad_request(err.to_string()),
    }
}

async fn pull_fray(State(state): State<AppState>, Path(fray): Path<String>) -> Response {
    let feed = match network::fetch_feed(state.lattice_rpc_port, &fray).await {
        Ok(Some(feed)) => feed,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error":"no network feed found for fray"})),
            )
                .into_response()
        }
        Err(err) => return bad_request(err.to_string()),
    };

    let mut imported = 0usize;
    for post in feed.posts {
        if state.store.upsert_post(post).is_ok() {
            imported += 1;
        }
    }
    if let Err(err) = state.store.flush() {
        return bad_request(err.to_string());
    }
    Json(json!({
      "status":"ok",
      "fray": fray,
      "imported_posts": imported,
    }))
    .into_response()
}

fn bad_request(message: String) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
}
