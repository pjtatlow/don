//! AWS SSM Parameter Store backend via the `aws` CLI.

use super::error::SecretError;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::process::Stdio;
use tokio::process::Command;

/// SSM `GetParameters` accepts at most 10 names per call.
const SSM_BATCH_SIZE: usize = 10;

pub struct AwsSsm {
    pub region: Option<String>,
    pub profile: Option<String>,
    pub(crate) program: String,
}

impl AwsSsm {
    pub fn new(region: Option<String>, profile: Option<String>) -> Self {
        Self {
            region,
            profile,
            program: std::env::var("KEY_AWS").unwrap_or_else(|_| "aws".to_string()),
        }
    }

    /// Fetch mapped paths. Returns env-name → value. Never includes values in errors.
    pub async fn fetch(
        &self,
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
        let mut paths: Vec<&str> = Vec::new();
        for path in subset.values() {
            if seen.insert(path.as_str()) {
                paths.push(path.as_str());
            }
        }

        let mut by_path: HashMap<String, String> = HashMap::new();
        for chunk in paths.chunks(SSM_BATCH_SIZE) {
            let output = self.get_parameters(chunk).await?;
            if !output.invalid_parameters.is_empty() {
                return Err(SecretError::msg(format!(
                    "AWS SSM parameters not found: {}",
                    output.invalid_parameters.join(", ")
                )));
            }
            for parameter in output.parameters {
                by_path.insert(parameter.name, parameter.value);
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

    async fn get_parameters(&self, names: &[&str]) -> Result<SsmGetParametersOutput, SecretError> {
        let mut cmd = Command::new(&self.program);
        cmd.arg("ssm")
            .arg("get-parameters")
            .arg("--with-decryption")
            .arg("--output")
            .arg("json")
            .arg("--names")
            .args(names.iter().copied())
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(region) = &self.region {
            cmd.arg("--region").arg(region);
        }
        if let Some(profile) = &self.profile {
            cmd.arg("--profile").arg(profile);
        }

        let output = cmd.output().await.map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                SecretError::msg(
                    "aws CLI not found on PATH; install the AWS CLI to use provider = \"aws-ssm\"",
                )
            } else {
                SecretError::msg(format!("failed to run aws ssm get-parameters: {error}"))
            }
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(SecretError::msg(format_aws_failure(
                stderr.trim(),
                self.profile.as_deref(),
            )));
        }

        serde_json::from_slice(&output.stdout).map_err(|error| {
            SecretError::msg(format!(
                "failed to parse aws ssm get-parameters output: {error}"
            ))
        })
    }
}

#[derive(Deserialize)]
struct SsmGetParametersOutput {
    #[serde(rename = "Parameters", default)]
    parameters: Vec<SsmParameter>,
    #[serde(rename = "InvalidParameters", default)]
    invalid_parameters: Vec<String>,
}

#[derive(Deserialize)]
struct SsmParameter {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Value")]
    value: String,
}

fn format_aws_failure(stderr: &str, profile: Option<&str>) -> String {
    let lower = stderr.to_ascii_lowercase();
    let sso = lower.contains("sso")
        || lower.contains("token has expired")
        || lower.contains("unable to locate credentials")
        || lower.contains("nocredentials")
        || lower.contains("expiredtoken")
        || lower.contains("error loading sso");
    let detail = if stderr.is_empty() {
        "aws ssm get-parameters failed".to_string()
    } else {
        stderr.lines().next().unwrap_or(stderr).trim().to_string()
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

    #[test]
    fn sso_errors_suggest_login() {
        let got = format_aws_failure("Token has expired and refresh failed", Some("dev"));
        assert!(got.contains("aws sso login --profile dev"), "{got}");
        let got = format_aws_failure("Unable to locate credentials", None);
        assert!(got.contains("aws sso login"), "{got}");
        let got = format_aws_failure("AccessDeniedException: User is not authorized", Some("dev"));
        assert_eq!(
            got,
            "AWS SSM: AccessDeniedException: User is not authorized"
        );
        assert!(!got.contains("sk_"));
    }

    #[tokio::test]
    async fn fetch_maps_paths_to_names_via_cli() {
        let dir = tempfile::tempdir().unwrap();
        let aws = dir.path().join("aws");
        std::fs::write(
            &aws,
            r#"#!/usr/bin/env python3
import json, sys
args = sys.argv[1:]
names = []
if "--names" in args:
    i = args.index("--names") + 1
    while i < len(args) and not args[i].startswith("-"):
        names.append(args[i])
        i += 1
json.dump(
    {
        "Parameters": [{"Name": n, "Value": f"value-for-{n}"} for n in names],
        "InvalidParameters": [],
    },
    sys.stdout,
)
"#,
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&aws).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&aws, perms).unwrap();
        }
        let mut vars = HashMap::new();
        vars.insert(
            "STRIPE_SECRET_KEY".to_string(),
            "/app/StripeSecretKey".to_string(),
        );
        vars.insert("DD_API_KEY".to_string(), "/app/Datadog/ApiKey".to_string());
        let fetched = AwsSsm {
            region: Some("us-east-1".into()),
            profile: Some("dev".into()),
            program: aws.to_string_lossy().into_owned(),
        }
        .fetch(&vars, &["STRIPE_SECRET_KEY".into(), "DD_API_KEY".into()])
        .await
        .unwrap();
        assert_eq!(
            fetched["STRIPE_SECRET_KEY"],
            "value-for-/app/StripeSecretKey"
        );
        assert_eq!(fetched["DD_API_KEY"], "value-for-/app/Datadog/ApiKey");
    }
}
