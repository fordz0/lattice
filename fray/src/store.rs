use crate::model::{CreatePostRequest, Post, PostSummary};
use crate::routes::{FrayName, FrayRouteError, Username};
use anyhow::{anyhow, Context, Result};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const POST_PREFIX: &str = "post:";

#[derive(Clone)]
pub struct FrayStore {
    db: sled::Db,
}

impl FrayStore {
    pub fn open(path: &Path) -> Result<Self> {
        let db = sled::open(path)
            .with_context(|| format!("failed to open fray db at {}", path.display()))?;
        Ok(Self { db })
    }

    pub fn create_post(&self, fray: &str, req: CreatePostRequest) -> Result<Post> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let author = Username::parse(&req.author).map_err(map_route_error)?;
        validate_title(&req.title)?;
        validate_body(&req.body)?;

        let created_at = now_secs()?;
        let id = new_post_id(created_at)?;
        let post = Post {
            id: id.clone(),
            fray: fray_name.as_str().to_string(),
            author: author.as_str().to_string(),
            title: req.title.trim().to_string(),
            body: req.body.trim().to_string(),
            created_at,
        };

        let key = post_key(&id);
        let encoded = serde_json::to_vec(&post).context("failed to serialize post")?;
        self.db
            .insert(key.as_bytes(), encoded)
            .context("failed to persist post")?;
        self.db.flush().context("failed to flush post write")?;
        Ok(post)
    }

    pub fn get_post(&self, fray: &str, post_id: &str) -> Result<Option<Post>> {
        FrayName::parse(fray).map_err(map_route_error)?;
        validate_post_id(post_id)?;
        let key = post_key(post_id);
        let Some(value) = self
            .db
            .get(key.as_bytes())
            .context("failed to read post from db")?
        else {
            return Ok(None);
        };

        let post: Post = serde_json::from_slice(&value).context("failed to decode post")?;
        if post.fray != fray {
            return Ok(None);
        }
        Ok(Some(post))
    }

    pub fn list_posts(&self, fray: &str, limit: usize) -> Result<Vec<PostSummary>> {
        let posts = self.list_posts_full(fray, limit)?;
        Ok(posts.iter().map(PostSummary::from).collect())
    }

    pub fn list_posts_full(&self, fray: &str, limit: usize) -> Result<Vec<Post>> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let max = limit.clamp(1, 200);
        let mut posts = Vec::new();
        for item in self.db.scan_prefix(POST_PREFIX.as_bytes()) {
            let (_key, value) = item.context("failed to iterate posts")?;
            let post: Post = match serde_json::from_slice(&value) {
                Ok(p) => p,
                Err(_) => continue,
            };
            if post.fray == fray_name.as_str() {
                posts.push(post);
            }
        }
        posts.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| b.id.cmp(&a.id))
        });
        posts.truncate(max);
        Ok(posts)
    }

    pub fn upsert_post(&self, post: Post) -> Result<()> {
        FrayName::parse(&post.fray).map_err(map_route_error)?;
        Username::parse(&post.author).map_err(map_route_error)?;
        validate_title(&post.title)?;
        validate_body(&post.body)?;
        validate_post_id(&post.id)?;

        let now = now_secs()?;
        if post.created_at > now.saturating_add(300) {
            return Err(anyhow!("post timestamp too far in the future"));
        }

        let key = post_key(&post.id);
        let encoded = serde_json::to_vec(&post).context("failed to serialize post")?;
        self.db
            .insert(key.as_bytes(), encoded)
            .context("failed to upsert post")?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush().context("failed to flush fray db")?;
        Ok(())
    }
}

fn validate_title(title: &str) -> Result<()> {
    let t = title.trim();
    if t.len() < 3 || t.len() > 140 {
        return Err(anyhow!("title must be between 3 and 140 characters"));
    }
    Ok(())
}

fn validate_body(body: &str) -> Result<()> {
    let b = body.trim();
    if b.is_empty() || b.len() > 20_000 {
        return Err(anyhow!("body must be between 1 and 20000 characters"));
    }
    Ok(())
}

fn validate_post_id(post_id: &str) -> Result<()> {
    if post_id.len() < 6 || post_id.len() > 40 {
        return Err(anyhow!("invalid post id"));
    }
    if !post_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err(anyhow!("invalid post id"));
    }
    Ok(())
}

fn now_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs())
}

fn new_post_id(ts: u64) -> Result<String> {
    let mut random = [0_u8; 8];
    getrandom::getrandom(&mut random)
        .map_err(|e| anyhow!("failed to generate post id randomness: {e}"))?;
    Ok(format!("{ts:016x}-{}", hex::encode(random)))
}

fn map_route_error(err: FrayRouteError) -> anyhow::Error {
    anyhow!(err.to_string())
}

fn post_key(id: &str) -> String {
    format!("{POST_PREFIX}{id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_db_path() -> PathBuf {
        let mut random = [0_u8; 4];
        let _ = getrandom::getrandom(&mut random);
        std::env::temp_dir().join(format!("fray-store-test-{}", hex::encode(random)))
    }

    #[test]
    fn create_and_read_post_roundtrip() {
        let path = temp_db_path();
        let store = FrayStore::open(&path).expect("open db");
        let post = store
            .create_post(
                "lattice",
                CreatePostRequest {
                    author: "fordz0".to_string(),
                    title: "hello".to_string(),
                    body: "world".to_string(),
                },
            )
            .expect("create post");

        let loaded = store
            .get_post("lattice", &post.id)
            .expect("get post")
            .expect("post exists");
        assert_eq!(loaded.title, "hello");

        let listed = store.list_posts("lattice", 20).expect("list posts");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, post.id);
        let _ = std::fs::remove_dir_all(path);
    }
}
