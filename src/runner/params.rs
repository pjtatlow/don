//! Param value resolution for task runs.
//!
//! Given the user's supplied `--<param>=<value>` map and the task's declared
//! [`TaskParam`]s, produce the final `HashMap<String,String>` that the
//! runner will thread through [`task::spawn_task`](super::task::spawn_task)
//! for template substitution — or a human-readable error describing what's
//! wrong.
//!
//! Validation catches unknown keys, missing required values, bad int parses,
//! out-of-range ints, non-bool booleans, and unlisted choice values.

use std::collections::HashMap;

use crate::config::{self, Task};

/// Resolve the user-supplied `raw` param values against `task.params`.
///
/// - Unknown keys in `raw` produce an error.
/// - Missing required values (no `raw` entry, no `default`) produce an error.
/// - Values are coerced per [`ParamKind`](crate::config::ParamKind) and
///   checked against `choices` / `validate`.
///
/// Returns the final map keyed by param name, with every declared param
/// present when the call succeeds. Callers pass this directly to
/// [`crate::config::template::render`].
pub(crate) fn resolve_task_params(
    task_name: &str,
    task: &Task,
    raw: HashMap<String, String>,
) -> Result<HashMap<String, String>, String> {
    // Reject unknown keys up front so the user sees the typo before any of
    // the other validation fires.
    let declared: std::collections::HashSet<&str> =
        task.params.iter().map(|p| p.name.as_str()).collect();
    for key in raw.keys() {
        if !declared.contains(key.as_str()) {
            let declared_vec: Vec<&str> = task.params.iter().map(|p| p.name.as_str()).collect();
            let valid = if declared_vec.is_empty() {
                "task '{task_name}' declares no params".to_string()
            } else {
                format!("valid params: {}", declared_vec.join(", "))
            };
            return Err(format!(
                "unknown param '--{key}' for task '{task_name}' ({valid})"
            ));
        }
    }

    let mut resolved = HashMap::with_capacity(task.params.len());
    for p in &task.params {
        let supplied = raw.get(&p.name).cloned();
        let value = match supplied.or_else(|| p.default.clone()) {
            Some(v) => v,
            None if p.required => {
                return Err(format!(
                    "missing required param '--{}' for task '{task_name}'",
                    p.name
                ));
            }
            // Not required and no default — leave out of the map. Template
            // rendering will error on `{{name}}` references, which is the
            // right behavior: the config author is asserting that the
            // placeholder is safe to omit only when the command structure
            // allows it.
            None => continue,
        };
        let coerced = coerce(&p.name, task_name, p, &value)?;
        resolved.insert(p.name.clone(), coerced);
    }
    Ok(resolved)
}

/// Coerce / validate a single param value according to its kind.
fn coerce(
    name: &str,
    task_name: &str,
    p: &config::TaskParam,
    value: &str,
) -> Result<String, String> {
    match p.kind {
        config::ParamKind::String | config::ParamKind::Choice => {
            if !p.choices.is_empty() && !p.choices.contains(&value.to_string()) {
                return Err(format!(
                    "param '--{name}' for task '{task_name}': '{value}' is not among \
                     choices [{}]",
                    p.choices.join(", ")
                ));
            }
            Ok(value.to_string())
        }
        config::ParamKind::Int => {
            let n: i64 = value.parse().map_err(|_| {
                format!("param '--{name}' for task '{task_name}': '{value}' is not a valid integer")
            })?;
            if let Some(v) = p.validate {
                if let Some(min) = v.min
                    && n < min
                {
                    return Err(format!(
                        "param '--{name}' for task '{task_name}': {n} is less than min {min}"
                    ));
                }
                if let Some(max) = v.max
                    && n > max
                {
                    return Err(format!(
                        "param '--{name}' for task '{task_name}': {n} is greater than max {max}"
                    ));
                }
            }
            Ok(n.to_string())
        }
        config::ParamKind::Bool => config::parse_bool_value(value)
            .map(|b| b.to_string())
            .ok_or_else(|| {
                format!("param '--{name}' for task '{task_name}': '{value}' is not a valid bool")
            }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::{CompletionParse, Completions, ParamKind, ParamValidate, TaskParam};

    fn empty_task() -> Task {
        Task {
            cmd: "echo".into(),
            args: vec![],
            dir: None,
            env: HashMap::new(),
            depends_on: vec![],
            watch: vec![],
            ignore: vec![],
            timeout: None,
            log: crate::config::LogConfig::Stdout,
            terminal: crate::config::TaskTerminal::default(),
            headless: None,
            auto_run: crate::config::TaskAutoRun::Always,
            reconcile_dependents: false,
            download: None,
            bazel: None,
            turbo: None,
            params: vec![],
            hidden: false,
            auto_filter_on_failure: None,
        }
    }

    fn task_with_params(params: Vec<TaskParam>) -> Task {
        let mut t = empty_task();
        t.params = params;
        t
    }

    fn basic_param(name: &str) -> TaskParam {
        TaskParam {
            name: name.to_string(),
            prompt: None,
            required: false,
            default: None,
            kind: ParamKind::String,
            choices: vec![],
            completions: None,
            validate: None,
        }
    }

    #[test]
    fn resolve_table() {
        struct Case {
            name: &'static str,
            params: Vec<TaskParam>,
            raw: &'static [(&'static str, &'static str)],
            want_ok: Option<&'static [(&'static str, &'static str)]>,
            want_err: Option<&'static str>,
        }

        let cases = vec![
            Case {
                name: "no params + no raw → empty",
                params: vec![],
                raw: &[],
                want_ok: Some(&[]),
                want_err: None,
            },
            Case {
                name: "unknown key rejected",
                params: vec![basic_param("a")],
                raw: &[("b", "x")],
                want_ok: None,
                want_err: Some("unknown param '--b'"),
            },
            Case {
                name: "required missing rejected",
                params: vec![TaskParam {
                    required: true,
                    ..basic_param("a")
                }],
                raw: &[],
                want_ok: None,
                want_err: Some("missing required"),
            },
            Case {
                name: "default applied when not supplied",
                params: vec![TaskParam {
                    default: Some("hello".into()),
                    ..basic_param("a")
                }],
                raw: &[],
                want_ok: Some(&[("a", "hello")]),
                want_err: None,
            },
            Case {
                name: "raw overrides default",
                params: vec![TaskParam {
                    default: Some("hello".into()),
                    ..basic_param("a")
                }],
                raw: &[("a", "world")],
                want_ok: Some(&[("a", "world")]),
                want_err: None,
            },
            Case {
                name: "optional without default is absent in result",
                params: vec![basic_param("a"), basic_param("b")],
                raw: &[("a", "x")],
                want_ok: Some(&[("a", "x")]),
                want_err: None,
            },
            Case {
                name: "int coerces and roundtrips canonical form",
                params: vec![TaskParam {
                    kind: ParamKind::Int,
                    ..basic_param("n")
                }],
                raw: &[("n", "42")],
                want_ok: Some(&[("n", "42")]),
                want_err: None,
            },
            Case {
                name: "int parse failure",
                params: vec![TaskParam {
                    kind: ParamKind::Int,
                    ..basic_param("n")
                }],
                raw: &[("n", "nope")],
                want_ok: None,
                want_err: Some("not a valid integer"),
            },
            Case {
                name: "int below min",
                params: vec![TaskParam {
                    kind: ParamKind::Int,
                    validate: Some(ParamValidate {
                        min: Some(10),
                        max: None,
                    }),
                    ..basic_param("n")
                }],
                raw: &[("n", "5")],
                want_ok: None,
                want_err: Some("less than min 10"),
            },
            Case {
                name: "int above max",
                params: vec![TaskParam {
                    kind: ParamKind::Int,
                    validate: Some(ParamValidate {
                        min: None,
                        max: Some(10),
                    }),
                    ..basic_param("n")
                }],
                raw: &[("n", "99")],
                want_ok: None,
                want_err: Some("greater than max 10"),
            },
            Case {
                name: "bool accepts 'true'",
                params: vec![TaskParam {
                    kind: ParamKind::Bool,
                    ..basic_param("b")
                }],
                raw: &[("b", "true")],
                want_ok: Some(&[("b", "true")]),
                want_err: None,
            },
            Case {
                name: "bool accepts 'yes' and canonicalizes",
                params: vec![TaskParam {
                    kind: ParamKind::Bool,
                    ..basic_param("b")
                }],
                raw: &[("b", "yes")],
                want_ok: Some(&[("b", "true")]),
                want_err: None,
            },
            Case {
                name: "bool rejects garbage",
                params: vec![TaskParam {
                    kind: ParamKind::Bool,
                    ..basic_param("b")
                }],
                raw: &[("b", "maybe")],
                want_ok: None,
                want_err: Some("not a valid bool"),
            },
            Case {
                name: "choice value among list",
                params: vec![TaskParam {
                    choices: vec!["a".into(), "b".into()],
                    ..basic_param("c")
                }],
                raw: &[("c", "a")],
                want_ok: Some(&[("c", "a")]),
                want_err: None,
            },
            Case {
                name: "choice value outside list",
                params: vec![TaskParam {
                    choices: vec!["a".into(), "b".into()],
                    ..basic_param("c")
                }],
                raw: &[("c", "z")],
                want_ok: None,
                want_err: Some("is not among choices"),
            },
            Case {
                name: "dynamic choice (completions) accepts free text",
                params: vec![TaskParam {
                    completions: Some(Completions {
                        cmd: "ls".into(),
                        args: vec![],
                        parse: CompletionParse::Lines,
                        cache: None,
                        timeout: None,
                    }),
                    ..basic_param("c")
                }],
                raw: &[("c", "anything")],
                want_ok: Some(&[("c", "anything")]),
                want_err: None,
            },
        ];

        for case in cases {
            let task = task_with_params(case.params);
            let raw: HashMap<String, String> = case
                .raw
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let got = resolve_task_params("t", &task, raw);
            match (got, case.want_ok, case.want_err) {
                (Ok(m), Some(want), None) => {
                    let want_map: HashMap<String, String> = want
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                    assert_eq!(m, want_map, "{}", case.name);
                }
                (Err(e), None, Some(needle)) => {
                    assert!(
                        e.contains(needle),
                        "{}: error '{e}' missing '{needle}'",
                        case.name
                    );
                }
                (got, want_ok, want_err) => panic!(
                    "{}: got {:?}, want ok={:?} err={:?}",
                    case.name, got, want_ok, want_err
                ),
            }
        }
    }
}
