/// Errors from config, fetch, or apply. Messages must never include values.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SecretError(String);

impl SecretError {
    pub fn msg(text: impl Into<String>) -> Self {
        Self(text.into())
    }
}
