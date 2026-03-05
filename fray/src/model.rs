use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Post {
    pub id: String,
    pub fray: String,
    pub author: String,
    pub title: String,
    pub body: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreatePostRequest {
    pub author: String,
    pub title: String,
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
