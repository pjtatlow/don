//! AWS SSM Parameter Store backend via the AWS SDK.

use super::error::SecretError;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_ssm::Client;
use futures_util::{StreamExt, TryStreamExt, stream};
use std::collections::{HashMap, HashSet};
use std::future::Future;

/// SSM `GetParameters` accepts at most 10 names per call.
const SSM_BATCH_SIZE: usize = 10;
// Bound pressure on SSM while avoiding a round trip per batch in series.
const SSM_CONCURRENCY: usize = 4;

pub struct AwsSsm {
    pub region: Option<String>,
    pub profile: Option<String>,
}

pub(crate) struct GetParametersResult {
    pub parameters: Vec<(String, String)>,
    pub invalid_parameters: Vec<String>,
}

/// One `GetParameters` round-trip. Production uses the AWS SDK; tests inject a mock.
pub(crate) trait SsmGetParameters: Send + Sync {
    fn get_parameters(
        &self,
        names: &[String],
    ) -> impl Future<Output = Result<GetParametersResult, SecretError>> + Send;
}

struct SdkSsm {
    client: Client,
    profile: Option<String>,
}

impl AwsSsm {
    pub fn new(region: Option<String>, profile: Option<String>) -> Self {
        Self { region, profile }
    }

    /// Fetch mapped paths. Returns env-name → value. Never includes values in errors.
    pub async fn fetch(
        &self,
        vars: &HashMap<String, String>,
        names: &[String],
    ) -> Result<HashMap<String, String>, SecretError> {
        let client = SdkSsm::connect(self.region.as_deref(), self.profile.as_deref()).await;
        fetch_with(&client, vars, names).await
    }
}

impl SdkSsm {
    async fn connect(region: Option<&str>, profile: Option<&str>) -> Self {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(Region::new(region.to_string()));
        }
        if let Some(profile) = profile {
            loader = loader.profile_name(profile);
        }
        let config = loader.load().await;
        Self {
            client: Client::new(&config),
            profile: profile.map(str::to_string),
        }
    }
}

impl SsmGetParameters for SdkSsm {
    async fn get_parameters(&self, names: &[String]) -> Result<GetParametersResult, SecretError> {
        let output = self
            .client
            .get_parameters()
            .set_names(Some(names.to_vec()))
            .with_decryption(true)
            .send()
            .await
            .map_err(|error| {
                SecretError::msg(format_aws_failure(
                    &error.to_string(),
                    self.profile.as_deref(),
                ))
            })?;

        Ok(GetParametersResult {
            parameters: output
                .parameters()
                .iter()
                .filter_map(|parameter| {
                    Some((
                        parameter.name()?.to_string(),
                        parameter.value()?.to_string(),
                    ))
                })
                .collect(),
            invalid_parameters: output.invalid_parameters().to_vec(),
        })
    }
}

async fn fetch_with<C: SsmGetParameters>(
    client: &C,
    vars: &HashMap<String, String>,
    names: &[String],
) -> Result<HashMap<String, String>, SecretError> {
    let mut subset = HashMap::new();
    for name in names {
        let Some(path) = vars.get(name) else {
            return Err(SecretError::msg(format!("unknown secret '{name}'")));
        };
        subset.insert(name.clone(), path.clone());
    }

    let mut seen = HashSet::new();
    let mut paths: Vec<String> = Vec::new();
    for path in subset.values() {
        if seen.insert(path.as_str()) {
            paths.push(path.clone());
        }
    }

    let mut by_path: HashMap<String, String> = HashMap::new();
    let requests = paths
        .chunks(SSM_BATCH_SIZE)
        .map(|chunk| client.get_parameters(chunk))
        .collect::<Vec<_>>();
    let mut batches = stream::iter(requests).buffer_unordered(SSM_CONCURRENCY);
    // These futures are owned here: an error or shutdown drops every in-flight
    // request, with no detached work left fetching secrets after cancellation.
    while let Some(output) = batches.try_next().await? {
        if !output.invalid_parameters.is_empty() {
            return Err(SecretError::msg(format!(
                "AWS SSM parameters not found: {}",
                output.invalid_parameters.join(", ")
            )));
        }
        for (name, value) in output.parameters {
            by_path.insert(name, value);
        }
    }

    let mut values = HashMap::new();
    for (name, path) in subset {
        let Some(value) = by_path.get(&path) else {
            return Err(SecretError::msg(format!(
                "AWS SSM did not return parameter '{path}' (mapped from '{name}')"
            )));
        };
        values.insert(name, value.clone());
    }
    Ok(values)
}

fn format_aws_failure(error: &str, profile: Option<&str>) -> String {
    let lower = error.to_ascii_lowercase();
    let sso = lower.contains("sso")
        || lower.contains("token has expired")
        || lower.contains("unable to locate credentials")
        || lower.contains("nocredentials")
        || lower.contains("credentialsnotloaded")
        || lower.contains("expiredtoken")
        || lower.contains("error loading sso")
        || lower.contains("failed to load credentials");
    let detail = if error.is_empty() {
        "failed to fetch SSM parameters".to_string()
    } else {
        error.lines().next().unwrap_or(error).trim().to_string()
    };
    if sso {
        let login = match profile {
            Some(profile) => format!("aws sso login --profile {profile}"),
            None => "aws sso login".to_string(),
        };
        format!("{detail}\nrun: {login}")
    } else {
        format!("AWS SSM: {detail}")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct MockSsm {
        values: HashMap<String, String>,
        invalid: Vec<String>,
        error: Option<String>,
        batches: Mutex<Vec<Vec<String>>>,
    }

    impl SsmGetParameters for MockSsm {
        async fn get_parameters(
            &self,
            names: &[String],
        ) -> Result<GetParametersResult, SecretError> {
            self.batches.lock().unwrap().push(names.to_vec());
            if let Some(error) = &self.error {
                return Err(SecretError::msg(format_aws_failure(error, Some("dev"))));
            }
            let mut parameters = Vec::new();
            let mut invalid_parameters = Vec::new();
            for name in names {
                if self.invalid.iter().any(|path| path == name) {
                    invalid_parameters.push(name.clone());
                    continue;
                }
                if let Some(value) = self.values.get(name) {
                    parameters.push((name.clone(), value.clone()));
                }
            }
            Ok(GetParametersResult {
                parameters,
                invalid_parameters,
            })
        }
    }

    struct DelayedSsm {
        mock: MockSsm,
        active: AtomicUsize,
        peak: AtomicUsize,
    }

    struct InFlight<'a>(&'a AtomicUsize);

    impl Drop for InFlight<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl SsmGetParameters for DelayedSsm {
        async fn get_parameters(
            &self,
            names: &[String],
        ) -> Result<GetParametersResult, SecretError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _flight = InFlight(&self.active);
            self.peak.fetch_max(active, Ordering::SeqCst);
            // Uneven completion times also exercise out-of-order responses.
            tokio::time::sleep(Duration::from_millis(10 + names.len() as u64)).await;
            self.mock.get_parameters(names).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_batches_are_bounded_and_cancelled_on_exit() {
        struct Case {
            count: usize,
            error: bool,
            invalid: bool,
            cancel: bool,
        }
        let cases = vec![
            Case {
                count: 1,
                error: false,
                invalid: false,
                cancel: false,
            },
            Case {
                count: 11,
                error: false,
                invalid: false,
                cancel: false,
            },
            Case {
                count: 99,
                error: false,
                invalid: false,
                cancel: false,
            },
            Case {
                count: 101,
                error: false,
                invalid: false,
                cancel: false,
            },
            Case {
                count: 99,
                error: true,
                invalid: false,
                cancel: false,
            },
            Case {
                count: 99,
                error: false,
                invalid: true,
                cancel: false,
            },
            Case {
                count: 99,
                error: false,
                invalid: false,
                cancel: true,
            },
        ];
        for case in cases {
            let vars: HashMap<String, String> = (0..case.count)
                .map(|i| (format!("K{i}"), format!("/app/P{i}")))
                .collect();
            let expected: HashMap<String, String> = (0..case.count)
                .map(|i| (format!("K{i}"), format!("v{i}")))
                .collect();
            let client = DelayedSsm {
                mock: MockSsm {
                    values: (0..case.count)
                        .map(|i| (format!("/app/P{i}"), format!("v{i}")))
                        .collect(),
                    invalid: if case.invalid {
                        vars.values().cloned().collect()
                    } else {
                        Vec::new()
                    },
                    error: case.error.then(|| "access denied".into()),
                    batches: Mutex::new(Vec::new()),
                },
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            };
            let names = vars.keys().cloned().collect::<Vec<_>>();
            let start = tokio::time::Instant::now();
            let timeout = Duration::from_millis(if case.cancel { 5 } else { 1000 });
            let result = tokio::time::timeout(timeout, fetch_with(&client, &vars, &names)).await;
            if case.cancel {
                assert!(result.is_err());
            } else if case.error || case.invalid {
                assert!(result.unwrap().is_err());
            } else {
                assert_eq!(result.unwrap().unwrap(), expected);
                let waves = case
                    .count
                    .div_ceil(SSM_BATCH_SIZE)
                    .div_ceil(SSM_CONCURRENCY);
                assert!(start.elapsed() <= Duration::from_millis(20 * waves as u64));
            }
            assert_eq!(client.active.load(Ordering::SeqCst), 0);
            assert_eq!(
                client.peak.load(Ordering::SeqCst),
                case.count.div_ceil(SSM_BATCH_SIZE).min(SSM_CONCURRENCY)
            );
        }
    }

    #[test]
    fn sso_errors_suggest_login() {
        let got = format_aws_failure("Token has expired and refresh failed", Some("dev"));
        assert!(got.contains("aws sso login --profile dev"), "{got}");
        let got = format_aws_failure("Unable to locate credentials", None);
        assert!(got.contains("aws sso login"), "{got}");
        let got = format_aws_failure(
            "CredentialsNotLoaded: failed to load credentials",
            Some("dev"),
        );
        assert!(got.contains("aws sso login --profile dev"), "{got}");
        let got = format_aws_failure("AccessDeniedException: User is not authorized", Some("dev"));
        assert_eq!(
            got,
            "AWS SSM: AccessDeniedException: User is not authorized"
        );
        assert!(!got.contains("sk_"));
    }

    #[tokio::test]
    async fn fetch_maps_paths_to_names() {
        let mut values = HashMap::new();
        values.insert(
            "/app/StripeSecretKey".to_string(),
            "value-for-/app/StripeSecretKey".to_string(),
        );
        values.insert(
            "/app/Datadog/ApiKey".to_string(),
            "value-for-/app/Datadog/ApiKey".to_string(),
        );
        let client = MockSsm {
            values,
            invalid: Vec::new(),
            error: None,
            batches: Mutex::new(Vec::new()),
        };
        let mut vars = HashMap::new();
        vars.insert(
            "STRIPE_SECRET_KEY".to_string(),
            "/app/StripeSecretKey".to_string(),
        );
        vars.insert("DD_API_KEY".to_string(), "/app/Datadog/ApiKey".to_string());
        let fetched = fetch_with(
            &client,
            &vars,
            &["STRIPE_SECRET_KEY".into(), "DD_API_KEY".into()],
        )
        .await
        .unwrap();
        assert_eq!(
            fetched["STRIPE_SECRET_KEY"],
            "value-for-/app/StripeSecretKey"
        );
        assert_eq!(fetched["DD_API_KEY"], "value-for-/app/Datadog/ApiKey");
        assert!(!format!("{fetched:?}").contains("sk_"));
    }

    #[tokio::test]
    async fn fetch_batches_at_ten_names() {
        let mut values = HashMap::new();
        let mut vars = HashMap::new();
        for i in 0..11 {
            let path = format!("/app/P{i}");
            values.insert(path.clone(), format!("v{i}"));
            vars.insert(format!("K{i}"), path);
        }
        let client = MockSsm {
            values,
            invalid: Vec::new(),
            error: None,
            batches: Mutex::new(Vec::new()),
        };
        let names: Vec<String> = (0..11).map(|i| format!("K{i}")).collect();
        fetch_with(&client, &vars, &names).await.unwrap();
        let batches = client.batches.lock().unwrap();
        assert_eq!(batches.len(), 2, "{batches:?}");
        assert_eq!(batches[0].len(), 10);
        assert_eq!(batches[1].len(), 1);
    }

    #[tokio::test]
    async fn missing_parameters_error_omits_values() {
        let client = MockSsm {
            values: HashMap::new(),
            invalid: vec!["/app/StripeSecretKey".into()],
            error: None,
            batches: Mutex::new(Vec::new()),
        };
        let mut vars = HashMap::new();
        vars.insert(
            "STRIPE_SECRET_KEY".to_string(),
            "/app/StripeSecretKey".to_string(),
        );
        let err = fetch_with(&client, &vars, &["STRIPE_SECRET_KEY".into()])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("/app/StripeSecretKey"), "{err}");
        assert!(!err.contains("sk_"));
    }

    #[tokio::test]
    async fn credential_errors_suggest_login_and_omit_values() {
        let client = MockSsm {
            values: HashMap::new(),
            invalid: Vec::new(),
            error: Some("the SSO session associated with this profile has expired".into()),
            batches: Mutex::new(Vec::new()),
        };
        let mut vars = HashMap::new();
        vars.insert("STRIPE_SECRET_KEY".to_string(), "/app/Stripe".to_string());
        let err = fetch_with(&client, &vars, &["STRIPE_SECRET_KEY".into()])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("aws sso login --profile dev"), "{err}");
        assert!(!err.contains("sk_live"));
    }
}
