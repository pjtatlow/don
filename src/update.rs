use std::time::Duration;

const CRATES_IO_API_BASE: &str = "https://crates.io/api/v1/crates";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpdateAvailable {
    pub(crate) current_version: String,
    pub(crate) latest_version: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpdateCheckError {
    #[error("failed to build update-check client: {0}")]
    Client(reqwest::Error),
    #[error("failed to query crates.io: {0}")]
    Request(reqwest::Error),
    #[error("failed to parse crates.io response: {0}")]
    Json(serde_json::Error),
}

#[derive(Debug, serde::Deserialize)]
struct CratesIoResponse {
    #[serde(rename = "crate")]
    crate_info: CratesIoCrate,
}

#[derive(Debug, serde::Deserialize)]
struct CratesIoCrate {
    max_stable_version: Option<String>,
}

pub(crate) async fn check_crates_io(
    crate_name: &str,
    current_version: &str,
    timeout: Duration,
) -> Result<Option<UpdateAvailable>, UpdateCheckError> {
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .user_agent(format!(
            "don/{} ({})",
            env!("CARGO_PKG_VERSION"),
            env!("CARGO_PKG_REPOSITORY")
        ))
        .build()
        .map_err(UpdateCheckError::Client)?;

    let body = client
        .get(format!("{CRATES_IO_API_BASE}/{crate_name}"))
        .send()
        .await
        .map_err(UpdateCheckError::Request)?
        .error_for_status()
        .map_err(UpdateCheckError::Request)?
        .text()
        .await
        .map_err(UpdateCheckError::Request)?;
    let response: CratesIoResponse = serde_json::from_str(&body).map_err(UpdateCheckError::Json)?;

    Ok(response
        .crate_info
        .max_stable_version
        .filter(|latest| crate::version::is_newer_version(latest, current_version))
        .map(|latest_version| UpdateAvailable {
            current_version: current_version.to_string(),
            latest_version,
        }))
}
