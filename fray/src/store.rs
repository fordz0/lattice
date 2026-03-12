use crate::model::{
    Comment, CommentSummary, CreateCommentRequest, CreatePostRequest, Post, PostSummary,
};
use crate::routes::{FrayName, FrayRouteError, Username};
use crate::directory::SignedFrayDirectory;
use crate::trust::{KeyRecord, KeyStanding, SignedTrustRecord};
use anyhow::{anyhow, Context, Result};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const POST_PREFIX: &str = "post:";
const COMMENT_PREFIX: &str = "comment:";
const POST_INDEX_PREFIX: &str = "idx:post:";
const COMMENT_INDEX_PREFIX: &str = "idx:comment:";
const TRUST_PREFIX: &str = "trust:";
const TRUST_RECORD_PREFIX: &str = "trust_record:";
const DIRECTORY_KEY: &str = "directory:current";
const OWNERSHIP_PREFIX: &str = "owner:";

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
        validate_post_body(&req.body)?;

        let created_at = now_secs()?;
        let id = new_object_id(created_at)?;
        let post = Post {
            id: id.clone(),
            fray: fray_name.as_str().to_string(),
            author: author.as_str().to_string(),
            title: req.title.trim().to_string(),
            body: req.body.trim().to_string(),
            created_at,
            publisher_key_b64: None,
            hidden: false,
        };

        self.write_post(post.clone(), true)?;
        Ok(post)
    }

    pub fn get_post(&self, fray: &str, post_id: &str) -> Result<Option<Post>> {
        FrayName::parse(fray).map_err(map_route_error)?;
        validate_object_id(post_id)?;
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
        if post.hidden {
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
        let prefix = post_index_prefix(fray_name.as_str());
        let max = limit.clamp(1, 200);
        let mut posts = Vec::new();

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            let (_idx_key, post_id_value) = item.context("failed to iterate post index")?;
            let post_id = std::str::from_utf8(&post_id_value).unwrap_or_default();
            if post_id.is_empty() {
                continue;
            }
            let Some(post) = self.get_post(fray_name.as_str(), post_id)? else {
                continue;
            };
            posts.push(post);
            if posts.len() >= max {
                break;
            }
        }
        Ok(posts)
    }

    pub fn create_comment(
        &self,
        fray: &str,
        post_id: &str,
        req: CreateCommentRequest,
    ) -> Result<Comment> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        validate_object_id(post_id)?;
        let author = Username::parse(&req.author).map_err(map_route_error)?;
        validate_comment_body(&req.body)?;

        let Some(post) = self.get_post(fray_name.as_str(), post_id)? else {
            return Err(anyhow!("post not found"));
        };
        if post.fray != fray_name.as_str() {
            return Err(anyhow!("post/fray mismatch"));
        }

        let created_at = now_secs()?;
        let id = new_object_id(created_at)?;
        let comment = Comment {
            id: id.clone(),
            fray: fray_name.as_str().to_string(),
            post_id: post_id.to_string(),
            author: author.as_str().to_string(),
            body: req.body.trim().to_string(),
            created_at,
            publisher_key_b64: None,
            hidden: false,
        };

        self.write_comment(comment.clone(), true)?;
        Ok(comment)
    }

    pub fn list_comments(
        &self,
        fray: &str,
        post_id: &str,
        limit: usize,
    ) -> Result<Vec<CommentSummary>> {
        let comments = self.list_comments_full(fray, post_id, limit)?;
        Ok(comments.iter().map(CommentSummary::from).collect())
    }

    pub fn list_comments_full(
        &self,
        fray: &str,
        post_id: &str,
        limit: usize,
    ) -> Result<Vec<Comment>> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        validate_object_id(post_id)?;
        let prefix = comment_index_prefix(fray_name.as_str(), post_id);
        let max = limit.clamp(1, 500);
        let mut comments = Vec::new();

        for item in self.db.scan_prefix(prefix.as_bytes()) {
            let (_idx_key, comment_id_value) = item.context("failed to iterate comment index")?;
            let comment_id = std::str::from_utf8(&comment_id_value).unwrap_or_default();
            if comment_id.is_empty() {
                continue;
            }
            let Some(comment) = self.get_comment(comment_id)? else {
                continue;
            };
            if comment.fray == fray_name.as_str() && comment.post_id == post_id {
                comments.push(comment);
            }
            if comments.len() >= max {
                break;
            }
        }
        Ok(comments)
    }

    pub fn collect_comments_for_posts(
        &self,
        fray: &str,
        post_ids: &[String],
        per_post_limit: usize,
    ) -> Result<Vec<Comment>> {
        let mut out = Vec::new();
        for post_id in post_ids {
            let comments = self.list_comments_full(fray, post_id, per_post_limit)?;
            out.extend(comments);
        }
        Ok(out)
    }

    pub fn upsert_post(&self, post: Post) -> Result<()> {
        self.write_post(post, false)
    }

    pub fn upsert_comment(&self, comment: Comment) -> Result<()> {
        self.write_comment(comment, false)
    }

    pub fn flush(&self) -> Result<()> {
        self.db.flush().context("failed to flush fray db")?;
        Ok(())
    }

    pub fn set_key_standing(&self, fray: &str, key_b64: &str, standing: KeyStanding) -> Result<()> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let record = KeyRecord {
            key_b64: key_b64.to_string(),
            standing,
            label: None,
            updated_at: now_secs()?,
        };
        let key = trust_key(fray_name.as_str(), key_b64);
        let value = serde_json::to_vec(&record).context("failed to serialize key standing")?;
        self.db
            .insert(key.as_bytes(), value)
            .context("failed to write key standing")?;
        self.db.flush().context("failed to flush key standing")?;
        Ok(())
    }

    pub fn store_trust_record(&self, fray: &str, trust: &SignedTrustRecord) -> Result<()> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let key = trust_record_key(fray_name.as_str());
        let value = serde_json::to_vec(trust).context("failed to serialize trust record")?;
        self.db
            .insert(key.as_bytes(), value)
            .context("failed to store trust record")?;
        self.db.flush().context("failed to flush trust record write")?;
        Ok(())
    }

    pub fn load_trust_record(&self, fray: &str) -> Result<Option<SignedTrustRecord>> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let key = trust_record_key(fray_name.as_str());
        let Some(value) = self.db.get(key.as_bytes()).context("failed to read trust record")? else {
            return Ok(None);
        };
        let record: SignedTrustRecord =
            serde_json::from_slice(&value).context("failed to decode trust record")?;
        Ok(Some(record))
    }

    pub fn store_key_record(&self, fray: &str, record: KeyRecord) -> Result<()> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let key = trust_key(fray_name.as_str(), &record.key_b64);
        let value = serde_json::to_vec(&record).context("failed to serialize key record")?;
        self.db
            .insert(key.as_bytes(), value)
            .context("failed to store key record")?;
        self.db.flush().context("failed to flush key record write")?;
        Ok(())
    }

    pub fn get_key_standing(&self, fray: &str, key_b64: &str) -> Result<Option<KeyStanding>> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let key = trust_key(fray_name.as_str(), key_b64);
        let Some(value) = self.db.get(key.as_bytes()).context("failed to read key standing")? else {
            return Ok(None);
        };
        let record: KeyRecord =
            serde_json::from_slice(&value).context("failed to decode key standing")?;
        Ok(Some(record.standing))
    }

    pub fn list_key_standings(&self, fray: &str) -> Result<Vec<KeyRecord>> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let prefix = trust_prefix(fray_name.as_str());
        let mut out = Vec::new();
        for item in self.db.scan_prefix(prefix.as_bytes()) {
            let (_key, value) = item.context("failed to iterate trust standings")?;
            let record: KeyRecord =
                serde_json::from_slice(&value).context("failed to decode trust standing")?;
            out.push(record);
        }
        out.sort_by(|a, b| a.key_b64.cmp(&b.key_b64));
        Ok(out)
    }

    pub fn store_directory(&self, dir: &SignedFrayDirectory) -> Result<()> {
        let value = serde_json::to_vec(dir).context("failed to serialize directory")?;
        self.db
            .insert(DIRECTORY_KEY.as_bytes(), value)
            .context("failed to store directory")?;
        self.db.flush().context("failed to flush directory store")?;
        Ok(())
    }

    pub fn load_directory(&self) -> Result<Option<SignedFrayDirectory>> {
        let Some(value) = self
            .db
            .get(DIRECTORY_KEY.as_bytes())
            .context("failed to load directory")?
        else {
            return Ok(None);
        };
        let directory: SignedFrayDirectory =
            serde_json::from_slice(&value).context("failed to decode directory")?;
        Ok(Some(directory))
    }

    pub fn store_fray_ownership(&self, fray: &str, owner_key_b64: &str) -> Result<()> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let key = ownership_key(fray_name.as_str());
        self.db
            .insert(key.as_bytes(), owner_key_b64.as_bytes())
            .context("failed to store fray ownership")?;
        self.db.flush().context("failed to flush fray ownership")?;
        Ok(())
    }

    pub fn get_fray_ownership(&self, fray: &str) -> Result<Option<String>> {
        let fray_name = FrayName::parse(fray).map_err(map_route_error)?;
        let key = ownership_key(fray_name.as_str());
        let Some(value) = self.db.get(key.as_bytes()).context("failed to read fray ownership")? else {
            return Ok(None);
        };
        let owner = std::str::from_utf8(&value)
            .context("invalid fray ownership encoding")?
            .to_string();
        Ok(Some(owner))
    }

    pub fn list_owned_frays(&self) -> Result<Vec<String>> {
        let mut frays = Vec::new();
        for item in self.db.scan_prefix(OWNERSHIP_PREFIX.as_bytes()) {
            let (key, _value) = item.context("failed to iterate ownership entries")?;
            let key_str = std::str::from_utf8(&key).context("invalid ownership key encoding")?;
            if let Some(fray) = key_str.strip_prefix(OWNERSHIP_PREFIX) {
                frays.push(fray.to_string());
            }
        }
        frays.sort();
        Ok(frays)
    }

    fn get_comment(&self, comment_id: &str) -> Result<Option<Comment>> {
        validate_object_id(comment_id)?;
        let key = comment_key(comment_id);
        let Some(value) = self
            .db
            .get(key.as_bytes())
            .context("failed to read comment from db")?
        else {
            return Ok(None);
        };
        let comment: Comment =
            serde_json::from_slice(&value).context("failed to decode comment")?;
        if comment.hidden {
            return Ok(None);
        }
        Ok(Some(comment))
    }

    fn write_post(&self, post: Post, flush: bool) -> Result<()> {
        FrayName::parse(&post.fray).map_err(map_route_error)?;
        Username::parse(&post.author).map_err(map_route_error)?;
        validate_title(&post.title)?;
        validate_post_body(&post.body)?;
        validate_object_id(&post.id)?;
        validate_timestamp(post.created_at)?;

        let post_key = post_key(&post.id);
        let post_value = serde_json::to_vec(&post).context("failed to serialize post")?;
        self.db
            .insert(post_key.as_bytes(), post_value)
            .context("failed to write post")?;

        let idx_key = post_index_key(&post.fray, post.created_at, &post.id)?;
        self.db
            .insert(idx_key.as_bytes(), post.id.as_bytes())
            .context("failed to write post index")?;

        if flush {
            self.db.flush().context("failed to flush post write")?;
        }
        Ok(())
    }

    fn write_comment(&self, comment: Comment, flush: bool) -> Result<()> {
        FrayName::parse(&comment.fray).map_err(map_route_error)?;
        Username::parse(&comment.author).map_err(map_route_error)?;
        validate_comment_body(&comment.body)?;
        validate_object_id(&comment.id)?;
        validate_object_id(&comment.post_id)?;
        validate_timestamp(comment.created_at)?;

        let comment_key = comment_key(&comment.id);
        let comment_value = serde_json::to_vec(&comment).context("failed to serialize comment")?;
        self.db
            .insert(comment_key.as_bytes(), comment_value)
            .context("failed to write comment")?;

        let idx_key = comment_index_key(
            &comment.fray,
            &comment.post_id,
            comment.created_at,
            &comment.id,
        )?;
        self.db
            .insert(idx_key.as_bytes(), comment.id.as_bytes())
            .context("failed to write comment index")?;

        if flush {
            self.db.flush().context("failed to flush comment write")?;
        }
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

fn validate_post_body(body: &str) -> Result<()> {
    let b = body.trim();
    if b.is_empty() || b.len() > 4_000 {
        return Err(anyhow!("post body must be between 1 and 4000 characters"));
    }
    Ok(())
}

fn validate_comment_body(body: &str) -> Result<()> {
    let b = body.trim();
    if b.is_empty() || b.len() > 1_200 {
        return Err(anyhow!(
            "comment body must be between 1 and 1200 characters"
        ));
    }
    Ok(())
}

fn validate_object_id(id: &str) -> Result<()> {
    if id.len() < 6 || id.len() > 40 {
        return Err(anyhow!("invalid id"));
    }
    if !id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        return Err(anyhow!("invalid id"));
    }
    Ok(())
}

fn validate_timestamp(ts: u64) -> Result<()> {
    let now = now_secs()?;
    if ts > now.saturating_add(300) {
        return Err(anyhow!("timestamp too far in the future"));
    }
    Ok(())
}

fn now_secs() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs())
}

fn new_object_id(ts: u64) -> Result<String> {
    let mut random = [0_u8; 8];
    getrandom::getrandom(&mut random)
        .map_err(|e| anyhow!("failed to generate id randomness: {e}"))?;
    Ok(format!("{ts:016x}-{}", hex::encode(random)))
}

fn post_key(id: &str) -> String {
    format!("{POST_PREFIX}{id}")
}

fn comment_key(id: &str) -> String {
    format!("{COMMENT_PREFIX}{id}")
}

fn post_index_prefix(fray: &str) -> String {
    format!("{POST_INDEX_PREFIX}{fray}:")
}

fn post_index_key(fray: &str, created_at: u64, id: &str) -> Result<String> {
    let rev = reverse_ts(created_at)?;
    Ok(format!("{POST_INDEX_PREFIX}{fray}:{rev:016x}:{id}"))
}

fn comment_index_prefix(fray: &str, post_id: &str) -> String {
    format!("{COMMENT_INDEX_PREFIX}{fray}:{post_id}:")
}

fn comment_index_key(fray: &str, post_id: &str, created_at: u64, id: &str) -> Result<String> {
    let rev = reverse_ts(created_at)?;
    Ok(format!(
        "{COMMENT_INDEX_PREFIX}{fray}:{post_id}:{rev:016x}:{id}"
    ))
}

fn trust_prefix(fray: &str) -> String {
    format!("{TRUST_PREFIX}{fray}:")
}

fn trust_key(fray: &str, key_b64: &str) -> String {
    format!("{TRUST_PREFIX}{fray}:{key_b64}")
}

fn trust_record_key(fray: &str) -> String {
    format!("{TRUST_RECORD_PREFIX}{fray}")
}

fn ownership_key(fray: &str) -> String {
    format!("{OWNERSHIP_PREFIX}{fray}")
}

fn reverse_ts(ts: u64) -> Result<u64> {
    validate_timestamp(ts)?;
    Ok(u64::MAX - ts)
}

fn map_route_error(err: FrayRouteError) -> anyhow::Error {
    anyhow!(err.to_string())
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
    fn create_post_and_comment_roundtrip() {
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

        let comment = store
            .create_comment(
                "lattice",
                &post.id,
                CreateCommentRequest {
                    author: "fordz0".to_string(),
                    body: "nice".to_string(),
                },
            )
            .expect("create comment");

        let loaded_post = store
            .get_post("lattice", &post.id)
            .expect("get post")
            .expect("post exists");
        assert_eq!(loaded_post.title, "hello");

        let listed_posts = store.list_posts("lattice", 20).expect("list posts");
        assert_eq!(listed_posts.len(), 1);
        assert_eq!(listed_posts[0].id, post.id);

        let listed_comments = store
            .list_comments("lattice", &post.id, 20)
            .expect("list comments");
        assert_eq!(listed_comments.len(), 1);
        assert_eq!(listed_comments[0].id, comment.id);
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn trust_standing_roundtrip() {
        let path = temp_db_path();
        let store = FrayStore::open(&path).expect("open db");
        store
            .set_key_standing("lattice", "key-abc", KeyStanding::Trusted)
            .expect("set standing");
        assert_eq!(
            store
                .get_key_standing("lattice", "key-abc")
                .expect("get standing"),
            Some(KeyStanding::Trusted)
        );
        let standings = store.list_key_standings("lattice").expect("list standings");
        assert_eq!(standings.len(), 1);
        assert_eq!(standings[0].key_b64, "key-abc");
        let _ = std::fs::remove_dir_all(path);
    }
}
