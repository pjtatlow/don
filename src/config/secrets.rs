//! Who-gets-what on services and tasks. Mapping lives in `[secrets]` (see `crate::secrets`).

use crate::secrets::{Group, SecretsConfig, expand_secret_refs};
use std::collections::{HashMap, HashSet};

pub(crate) fn validate_secrets(
    secrets: &[SecretsConfig],
    services: &HashMap<String, super::Service>,
    tasks: &HashMap<String, super::Task>,
    service_groups: &HashMap<String, super::ServiceGroup>,
    suggest_typo: impl Fn(&str, &HashSet<&str>) -> String,
    errors: &mut Vec<String>,
) {
    let configured = !secrets.is_empty();
    if !configured {
        for (name, svc) in services {
            if svc.secrets.is_some() {
                errors.push(format!(
                    "service '{name}': secrets = [...] requires a [secrets] table"
                ));
            }
        }
        for (name, task) in tasks {
            if task.secrets.is_some() {
                errors.push(format!(
                    "task '{name}': secrets = [...] requires a [secrets] table"
                ));
            }
        }
        for (name, group) in service_groups {
            if !group.secrets.is_empty() {
                errors.push(format!(
                    "service group '{name}': secrets = [...] requires a [secrets] table"
                ));
            }
        }
        return;
    }

    for (i, source) in secrets.iter().enumerate() {
        for error in source.mapping_errors() {
            errors.push(format!("[[secrets]] #{}: {error}", i + 1));
        }
    }

    // A process may name anything any source supplies, so refs resolve against
    // the union rather than one entry.
    let mut var_names: HashSet<&str> = HashSet::new();
    let mut group_names: HashSet<&str> = HashSet::new();
    for source in secrets {
        var_names.extend(source.vars.keys().map(String::as_str));
        group_names.extend(source.groups.keys().map(String::as_str));
    }

    let mut all_groups: HashMap<String, Group> = HashMap::new();
    for source in secrets {
        all_groups.extend(source.groups.clone());
    }

    let mut candidates: HashSet<&str> = var_names.iter().copied().collect();
    candidates.extend(group_names.iter().copied());

    for (name, svc) in services {
        push_ref_errors(
            "service",
            name,
            svc.secrets.as_deref().unwrap_or(&[]),
            &var_names,
            &all_groups,
            &candidates,
            &suggest_typo,
            errors,
        );
        for (platform, ov) in &svc.platform {
            if let Some(refs) = &ov.secrets {
                push_ref_errors(
                    "service",
                    &format!("{name}.platform.{platform}"),
                    refs,
                    &var_names,
                    &all_groups,
                    &candidates,
                    &suggest_typo,
                    errors,
                );
            }
        }
    }
    for (name, task) in tasks {
        push_ref_errors(
            "task",
            name,
            task.secrets.as_deref().unwrap_or(&[]),
            &var_names,
            &all_groups,
            &candidates,
            &suggest_typo,
            errors,
        );
    }
    for (name, group) in service_groups {
        push_ref_errors(
            "service group",
            name,
            &group.secrets,
            &var_names,
            &all_groups,
            &candidates,
            &suggest_typo,
            errors,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn push_ref_errors(
    kind: &str,
    name: &str,
    refs: &[String],
    vars: &HashSet<&str>,
    groups: &HashMap<String, Group>,
    candidates: &HashSet<&str>,
    suggest_typo: impl Fn(&str, &HashSet<&str>) -> String,
    errors: &mut Vec<String>,
) {
    if let Err(unknown) = expand_secret_refs(refs, vars, groups) {
        for err in unknown {
            let suggestion = suggest_typo(err.name, candidates);
            errors.push(format!(
                "{kind} '{name}': unknown secret or secret group '{}' {suggestion}",
                err.name
            ));
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use crate::config::{Config, Platform};

    #[test]
    fn config_secrets_validation_table() {
        struct Case {
            name: &'static str,
            don: &'static str,
            want_err: Option<&'static str>,
        }
        let cases = [
            Case {
                name: "valid mapping and per-service list",
                don: r#"
                    [[secrets]]
                    aws-ssm = {}
                    [secrets.vars]
                    STRIPE_SECRET_KEY = "/app/StripeSecretKey"
                    DD_API_KEY = "/app/Datadog/ApiKey"
                    [secrets.groups]
                    app = ["STRIPE_SECRET_KEY"]
                    [services.api]
                    run.cmd = "true"
                    secrets = ["app"]
                    [services.web]
                    run.cmd = "true"
                "#,
                want_err: None,
            },
            Case {
                name: "unknown secret gets a suggestion",
                don: r#"
                    [[secrets]]
                    aws-ssm = {}
                    [secrets.vars]
                    STRIPE_SECRET_KEY = "/app/StripeSecretKey"
                    [services.api]
                    run.cmd = "true"
                    secrets = ["STRIPE_SECRET_KE"]
                "#,
                want_err: Some("did you mean 'STRIPE_SECRET_KEY'"),
            },
            Case {
                name: "secrets list without [secrets] table",
                don: r#"
                    [services.api]
                    run.cmd = "true"
                    secrets = ["STRIPE_SECRET_KEY"]
                "#,
                want_err: Some("requires a [secrets] table"),
            },
            Case {
                name: "ssm path must start with slash",
                don: r#"
                    [[secrets]]
                    aws-ssm = {}
                    [secrets.vars]
                    STRIPE_SECRET_KEY = "app/StripeSecretKey"
                    [services.api]
                    run.cmd = "true"
                "#,
                want_err: Some("must start with '/'"),
            },
        ];
        for case in cases {
            let config: Config = case.don.parse().unwrap();
            let result = config.validate(Platform::LinuxX86_64);
            match (case.want_err, result) {
                (None, Ok(_)) => {}
                (None, Err(e)) => panic!("{}: unexpected error {e}", case.name),
                (Some(needle), Err(e)) => {
                    let text = e.to_string();
                    assert!(
                        text.contains(needle),
                        "{}: expected {needle:?} in {text}",
                        case.name
                    );
                }
                (Some(needle), Ok(_)) => {
                    panic!("{}: expected error containing {needle:?}", case.name)
                }
            }
        }
    }
}
