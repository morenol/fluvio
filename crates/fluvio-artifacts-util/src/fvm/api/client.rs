//! Hub FVM API Client

use anyhow::{Result};
use octocrab::Octocrab;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::fvm::{Artifact, Channel, PackageSet};

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiError {
    pub status: u16,
    pub message: String,
}

/// HTTP Client for interacting with the Hub FVM API
#[derive(Debug, Default)]
pub struct Client;

impl Client {
    pub async fn fetch_package_set(&self, channel: &Channel, arch: &str) -> Result<PackageSet> {
        #[cfg(target_arch = "wasm32")]
        let octocrab = Octocrab::builder().with_service(client).build()?;
        #[cfg(not(target_arch = "wasm32"))]
        let octocrab = Octocrab::builder().build()?;

        let (release, version) = match channel {
            Channel::Stable => {
                // we have to fetch last release id from github
                let release = octocrab
                    .repos("fluvio-community", "fluvio")
                    .releases()
                    .get_latest()
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Unable to retrieve stable release for FVM: {e}")
                    })?;
                let version = Version::parse(&release.tag_name.trim_start_matches('v'))?;

                (release, version)
            }
            Channel::Tag(ver) => {
                let release_id = format!("v{}", ver);
                let release = octocrab
                    .repos("fluvio-community", "fluvio")
                    .releases()
                    .get_by_tag(&release_id)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Unable to retrieve release for tag {release_id} for FVM: {e}"
                        )
                    })?;
                (release, ver.clone())
            }
            Channel::Latest => {
                let release = octocrab
                    .repos("fluvio-community", "fluvio")
                    .releases()
                    .get_by_tag("dev")
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Unable to retrieve release for tag dev for FVM: {e}")
                    })?;

                let stable_release = octocrab
                    .repos("fluvio-community", "fluvio")
                    .releases()
                    .get_latest()
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Unable to retrieve stable release for FVM: {e}")
                    })?;
                let stable_version =
                    Version::parse(&stable_release.tag_name.trim_start_matches('v'))?;

                let version = Version {
                    pre: semver::Prerelease::new("dev").unwrap(),
                    patch: stable_version.patch + 1,
                    ..stable_version
                };
                (release, version)
            }
            Channel::Other(release) => {
                let release = octocrab
                    .repos("fluvio-community", "fluvio")
                    .releases()
                    .get_by_tag(release)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Unable to retrieve release for tag {release} for FVM: {e}")
                    })?;
                let version = Version::parse(&release.tag_name.trim_start_matches('v'))?;
                (release, version)
            }
        };

        let artifacts = release
            .assets
            .iter()
            .filter(|asset| asset.name.ends_with(&format!("{arch}.zip")))
            .map(|asset| Artifact {
                name: // remove -arch.zip from name
                    asset
                        .name
                        .trim_end_matches(&format!("-{arch}.zip"))
                        .to_string(),
                version: version.clone(),
                download_url: asset.browser_download_url.to_string(),
                sha256_url: "".to_string(),
            })
            .collect();

        let package_set = PackageSet {
            arch: arch.to_string(),
            pkgset: version,
            artifacts,
        };

        Ok(package_set)
    }
}
