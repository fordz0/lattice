use std::fmt;

const RESERVED_FRAY_NAMES: &[&str] = &["all", "admin", "api", "assets", "f", "mod", "u"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrayRoute {
    Home,
    Fray { fray: FrayName },
    Post { fray: FrayName, post: PostSlug },
    User { user: Username },
}

impl FrayRoute {
    pub fn parse(path: &str) -> Result<Self, FrayRouteError> {
        let trimmed = path.trim();
        if trimmed.is_empty() || trimmed == "/" {
            return Ok(Self::Home);
        }
        let clean_path = trimmed.split('?').next().unwrap_or(trimmed);
        let segments: Vec<&str> = clean_path.split('/').filter(|s| !s.is_empty()).collect();

        match segments.as_slice() {
            ["f", fray] => Ok(Self::Fray {
                fray: FrayName::parse(fray)?,
            }),
            ["f", fray, post] => Ok(Self::Post {
                fray: FrayName::parse(fray)?,
                post: PostSlug::parse(post)?,
            }),
            ["u", user] => Ok(Self::User {
                user: Username::parse(user)?,
            }),
            _ => Err(FrayRouteError::UnknownRoute),
        }
    }

    pub fn canonical_path(&self) -> String {
        match self {
            Self::Home => "/".to_string(),
            Self::Fray { fray } => format!("/f/{}", fray.as_str()),
            Self::Post { fray, post } => format!("/f/{}/{}", fray.as_str(), post.as_str()),
            Self::User { user } => format!("/u/{}", user.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrayName(String);

impl FrayName {
    pub fn parse(input: &str) -> Result<Self, FrayRouteError> {
        let value = normalize(input);
        validate_label(&value, 3, 32)?;
        if RESERVED_FRAY_NAMES.contains(&value.as_str()) {
            return Err(FrayRouteError::ReservedName);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Username(String);

impl Username {
    pub fn parse(input: &str) -> Result<Self, FrayRouteError> {
        let value = normalize(input);
        validate_label(&value, 3, 32)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostSlug(String);

impl PostSlug {
    pub fn parse(input: &str) -> Result<Self, FrayRouteError> {
        let value = normalize(input);
        validate_label(&value, 3, 96)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrayRouteError {
    UnknownRoute,
    InvalidLabel,
    ReservedName,
}

impl fmt::Display for FrayRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownRoute => write!(f, "unknown route"),
            Self::InvalidLabel => write!(f, "invalid label"),
            Self::ReservedName => write!(f, "reserved name"),
        }
    }
}

impl std::error::Error for FrayRouteError {}

fn normalize(input: &str) -> String {
    input.trim().trim_matches('/').to_ascii_lowercase()
}

fn validate_label(label: &str, min_len: usize, max_len: usize) -> Result<(), FrayRouteError> {
    if label.len() < min_len || label.len() > max_len {
        return Err(FrayRouteError::InvalidLabel);
    }
    if label.starts_with('-') || label.ends_with('-') || label.contains("--") {
        return Err(FrayRouteError::InvalidLabel);
    }
    if !label
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(FrayRouteError::InvalidLabel);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_home() {
        assert_eq!(FrayRoute::parse("/").unwrap(), FrayRoute::Home);
    }

    #[test]
    fn parses_fray_and_post() {
        assert_eq!(
            FrayRoute::parse("/f/lattice").unwrap(),
            FrayRoute::Fray {
                fray: FrayName("lattice".into())
            }
        );
        assert_eq!(
            FrayRoute::parse("/f/lattice/hello-world").unwrap(),
            FrayRoute::Post {
                fray: FrayName("lattice".into()),
                post: PostSlug("hello-world".into())
            }
        );
    }

    #[test]
    fn parses_user() {
        assert_eq!(
            FrayRoute::parse("/u/fordz0").unwrap(),
            FrayRoute::User {
                user: Username("fordz0".into())
            }
        );
    }

    #[test]
    fn rejects_reserved_frays() {
        assert_eq!(
            FrayName::parse("admin").unwrap_err(),
            FrayRouteError::ReservedName
        );
        assert_eq!(
            FrayName::parse("f").unwrap_err(),
            FrayRouteError::InvalidLabel
        );
    }

    #[test]
    fn rejects_invalid_labels() {
        assert_eq!(
            FrayRoute::parse("/f/bad.name").unwrap_err(),
            FrayRouteError::InvalidLabel
        );
        assert_eq!(
            FrayRoute::parse("/u/-bad").unwrap_err(),
            FrayRouteError::InvalidLabel
        );
    }
}
