//! The SSM CLI argument-file format: one argument per line, no shell expansion.

use std::collections::HashMap;

pub(super) struct Manifest {
    pub region: String,
    pub vars: HashMap<String, String>,
    pub keys: Vec<String>,
}

pub(super) fn parse(content: &str) -> Result<Manifest, String> {
    let mut lines = content.lines().enumerate();
    let mut region = None;
    let mut vars = HashMap::new();
    let mut keys = Vec::new();
    while let Some((line, argument)) = lines.next() {
        let error = |message: &str| format!("line {}: {message}", line + 1);
        match argument {
            "--region" => {
                if region.is_some() {
                    return Err(error("--region is repeated"));
                }
                let (_, value) = lines
                    .next()
                    .ok_or_else(|| error("--region needs a value"))?;
                if value.is_empty() || value.starts_with("--") {
                    return Err(error("--region needs a nonempty value"));
                }
                region = Some(value.to_string());
            }
            "--var" => {
                let (_, name) = lines
                    .next()
                    .ok_or_else(|| error("--var needs a name and path"))?;
                let (_, path) = lines.next().ok_or_else(|| error("--var needs a path"))?;
                if !super::config::is_valid_name(name) {
                    return Err(error("--var name must be an env-var identifier"));
                }
                if !path.starts_with('/') {
                    return Err(error("SSM parameter path must start with '/'"));
                }
                if let Some(previous) = vars.get(name) {
                    if previous != path {
                        return Err(error(&format!("variable '{name}' has conflicting paths")));
                    }
                } else {
                    keys.push(name.to_string());
                    vars.insert(name.to_string(), path.to_string());
                }
            }
            _ => return Err(error("expected --region or --var")),
        }
    }
    let region = region.ok_or("missing --region")?;
    if vars.is_empty() {
        return Err("no --var entries".into());
    }
    Ok(Manifest { region, vars, keys })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::parse;

    #[test]
    fn malformed_manifest_table() {
        for (content, message) in [
            ("--var\nKEY\n/app/key", "missing --region"),
            ("--region\nus-east-1", "no --var entries"),
            ("--region", "needs a value"),
            ("--region\n--var", "nonempty value"),
            ("--region\nus-east-1\n--region\nus-east-2", "repeated"),
            ("--region\nus-east-1\n--var\nKEY", "needs a path"),
            (
                "--region\nus-east-1\n--var\nBAD-KEY\n/app/key",
                "env-var identifier",
            ),
            ("--region\nus-east-1\n--var\nKEY\napp/key", "start with '/'"),
            (
                "--region\nus-east-1\n--var\nKEY\n/a\n--var\nKEY\n/b",
                "conflicting paths",
            ),
            ("--profile\ndev", "expected --region or --var"),
        ] {
            let error = parse(content).err().unwrap();
            assert!(error.contains(message), "expected {message} in {error}");
        }
    }
}
