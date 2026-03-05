use crate::model::CreatePostRequest;
use crate::store::FrayStore;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

#[derive(Clone)]
pub struct AppState {
    pub store: FrayStore,
}

#[derive(Debug, Deserialize)]
pub struct ListPostsQuery {
    pub limit: Option<usize>,
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/f/{fray}/posts", get(list_posts).post(create_post))
        .route("/api/f/{fray}/posts/{post_id}", get(get_post))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({"status":"ok"}))
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

fn bad_request(message: String) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": message }))).into_response()
}
