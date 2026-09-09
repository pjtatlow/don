//! Replace known secret values in log bytes. Values are never logged.

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use std::sync::{Arc, RwLock};

pub const MIN_VALUE_LEN: usize = 8;
const REPLACEMENT: &[u8] = b"***";

struct Engine {
    ac: Option<AhoCorasick>,
}

impl Engine {
    fn from_values<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let patterns: Vec<&str> = values
            .into_iter()
            .filter(|v| v.len() >= MIN_VALUE_LEN)
            .collect();
        if patterns.is_empty() {
            return Self { ac: None };
        }
        let ac = AhoCorasickBuilder::new()
            .match_kind(MatchKind::LeftmostLongest)
            .build(&patterns)
            .ok();
        Self { ac }
    }

    fn redact(&self, input: &[u8]) -> Vec<u8> {
        let Some(ac) = &self.ac else {
            return input.to_vec();
        };
        let replace: Vec<&[u8]> = vec![REPLACEMENT; ac.patterns_len()];
        ac.replace_all_bytes(input, &replace)
    }
}

/// Shared, updatable redactor. Set after secrets are fetched, before spawn.
#[derive(Clone)]
pub struct SecretRedactor {
    inner: Arc<RwLock<Engine>>,
}

impl Default for SecretRedactor {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Engine { ac: None })),
        }
    }
}

impl std::fmt::Debug for SecretRedactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretRedactor").finish_non_exhaustive()
    }
}

impl SecretRedactor {
    pub fn set_values<I, S>(&self, values: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let owned: Vec<String> = values.into_iter().map(|s| s.as_ref().to_string()).collect();
        let engine = Engine::from_values(owned.iter().map(String::as_str));
        if let Ok(mut guard) = self.inner.write() {
            *guard = engine;
        }
    }

    pub fn redact_bytes(&self, input: &[u8]) -> Vec<u8> {
        match self.inner.read() {
            Ok(engine) => engine.redact(input),
            Err(_) => input.to_vec(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn redacts_after_set() {
        let r = SecretRedactor::default();
        assert_eq!(r.redact_bytes(b"long-secret-value"), b"long-secret-value");
        r.set_values(["long-secret-value"]);
        assert_eq!(
            r.redact_bytes(b"see long-secret-value here"),
            b"see *** here"
        );
    }
}
