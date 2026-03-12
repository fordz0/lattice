use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Post {
    pub id: String,
    pub fray: String,
    pub author: String,
    pub title: String,
    pub body: String,
    pub created_at: u64,
    #[serde(default)]
    pub key_b64: Option<String>,
    #[serde(default)]
    pub signature_b64: Option<String>,
    #[serde(default)]
    pub hidden: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreatePostRequest {
    pub author: String,
    pub title: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Comment {
    pub id: String,
    pub fray: String,
    pub post_id: String,
    pub author: String,
    pub body: String,
    pub created_at: u64,
    #[serde(default)]
    pub key_b64: Option<String>,
    #[serde(default)]
    pub signature_b64: Option<String>,
    #[serde(default)]
    pub hidden: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateCommentRequest {
    pub author: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PostSummary {
    pub id: String,
    pub fray: String,
    pub author: String,
    pub title: String,
    pub created_at: u64,
}

impl From<&Post> for PostSummary {
    fn from(value: &Post) -> Self {
        Self {
            id: value.id.clone(),
            fray: value.fray.clone(),
            author: value.author.clone(),
            title: value.title.clone(),
            created_at: value.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CommentSummary {
    pub id: String,
    pub fray: String,
    pub post_id: String,
    pub author: String,
    pub body: String,
    pub created_at: u64,
}

impl From<&Comment> for CommentSummary {
    fn from(value: &Comment) -> Self {
        Self {
            id: value.id.clone(),
            fray: value.fray.clone(),
            post_id: value.post_id.clone(),
            author: value.author.clone(),
            body: value.body.clone(),
            created_at: value.created_at,
        }
    }
}
