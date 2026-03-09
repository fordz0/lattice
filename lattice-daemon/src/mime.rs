pub const ALLOWED_MIME_TYPES: &[&str] = &[
    "text/html",
    "text/css",
    "text/javascript",
    "application/javascript",
    "application/json",
    "image/png",
    "image/jpeg",
    "image/webp",
    "image/svg+xml",
    "image/gif",
    "font/woff2",
    "font/woff",
];

pub const MAX_FILE_BYTES: usize = 512_000;

pub fn is_allowed(mime: &str, size: usize) -> bool {
    ALLOWED_MIME_TYPES.contains(&mime) && size <= MAX_FILE_BYTES
}

pub fn violation_reason(mime: &str, size: usize) -> Option<&'static str> {
    if !ALLOWED_MIME_TYPES.contains(&mime) {
        return Some("wrong_type");
    }
    if size > MAX_FILE_BYTES {
        return Some("too_large");
    }
    None
}

pub fn detect_mime(path: &str, bytes: &[u8]) -> String {
    if let Some(kind) = infer::get(bytes) {
        let mime = kind.mime_type();
        if mime != "application/octet-stream" {
            return mime.to_string();
        }
    }

    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());

    match ext.as_deref() {
        Some("html") => "text/html".to_string(),
        Some("css") => "text/css".to_string(),
        Some("js") => "application/javascript".to_string(),
        Some("json") => "application/json".to_string(),
        Some("png") => "image/png".to_string(),
        Some("jpg") | Some("jpeg") => "image/jpeg".to_string(),
        Some("webp") => "image/webp".to_string(),
        Some("svg") => "image/svg+xml".to_string(),
        Some("gif") => "image/gif".to_string(),
        Some("woff2") => "font/woff2".to_string(),
        Some("woff") => "font/woff".to_string(),
        _ => "application/octet-stream".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_expected_types_and_sizes() {
        assert!(is_allowed("text/html", MAX_FILE_BYTES));
        assert!(is_allowed("image/png", 1024));
        assert!(!is_allowed("video/mp4", 1024));
        assert!(!is_allowed("text/html", MAX_FILE_BYTES + 1));
        assert_eq!(violation_reason("video/mp4", 1024), Some("wrong_type"));
        assert_eq!(
            violation_reason("text/html", MAX_FILE_BYTES + 1),
            Some("too_large")
        );
    }
}
