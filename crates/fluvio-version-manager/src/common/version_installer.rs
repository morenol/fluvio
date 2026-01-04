use std::path::{Path, PathBuf};
use std::fs::{copy, create_dir, remove_file, rename, File};

use anyhow::{anyhow, Result};
use tempfile::TempDir;

use octocrab::Octocrab;
use reqwest::Client;
use zip::ZipArchive;

use fluvio_hub_util::fvm::Channel;

use super::executable::set_executable_mode;
use super::manifest::{VersionManifest, VersionedArtifact};
use super::notify::Notify;
use super::version_directory::VersionDirectory;
use super::workdir::fvm_versions_path;
use crate::common::{DEFAULT_ARTIFACTS, TARGET};

pub struct VersionInstaller {
    channel: Channel,
    notify: Notify,
}

impl VersionInstaller {
    pub fn new(channel: Channel, notify: Notify) -> Self {
        Self { channel, notify }
    }

    pub async fn install(&self) -> Result<()> {
        let artifacts: Vec<String> = DEFAULT_ARTIFACTS.iter().map(|s| s.to_string()).collect();

        let (tmp_dir, release_version) = self.download(&artifacts).await?;
        let version_path = self.store_artifacts(&tmp_dir, &artifacts).await?;

        let contents = artifacts
            .iter()
            .map(|name| VersionedArtifact::new(name.to_owned(), release_version.to_string()))
            .collect::<Vec<VersionedArtifact>>();

        let manifest =
            VersionManifest::new(self.channel.to_owned(), release_version.clone(), contents);

        manifest.write(&version_path)?;
        self.notify
            .done(format!("Installed fluvio version {}", release_version));

        let version_dir = VersionDirectory::open(version_path)?;
        version_dir.set_active()?;

        self.notify
            .done(format!("Now using fluvio version {}", manifest.version));

        Ok(())
    }

    /// Downloads the specified artifacts to the temporary directory and
    /// returns a reference to the temporary directory [`TempDir`].
    ///
    /// The `tmp_dir` must be dropped after copying the binaries to the
    /// destination directory. By dropping [`TempDir`] the directory will be
    /// deleted from the filesystem.
    async fn download(&self, artifacts: &[String]) -> Result<(TempDir, semver::Version)> {
        // Use channel string as tag name for GitHub release
        let tag = self.channel.to_string();
        self.download_with_tag(artifacts, &tag).await
    }

    async fn download_with_tag(
        &self,
        artifacts: &[String],
        tag: &str,
    ) -> Result<(TempDir, semver::Version)> {
        let tmp_dir = TempDir::new()?;

        let octocrab = Octocrab::builder().build()?;

        let release = octocrab
            .repos("fluvio-community", "fluvio")
            .releases()
            .get_by_tag(tag)
            .await
            .map_err(|e| anyhow!("failed to get release {}: {}", tag, e))?;

        let tag_name = release.tag_name;
        // parse semver, tolerate leading 'v'
        let ver_str = tag_name.trim_start_matches('v');
        let release_version = semver::Version::parse(ver_str)?;

        // list assets - use releases API to list assets if necessary
        let release_id = release.id;
        let assets_page = octocrab
            .repos("fluvio-community", "fluvio")
            .releases()
            .assets(*release_id)
            .send()
            .await
            .map_err(|e| anyhow!("failed to list assets for {}: {}", tag, e))?;

        let client = Client::new();

        for (idx, base_name) in artifacts.iter().enumerate() {
            let asset_name = format!("{}-{}.zip", base_name, TARGET);

            self.notify.info(format!(
                "Downloading ({}/{}): {}",
                idx + 1,
                artifacts.len(),
                asset_name
            ));

            let asset_opt = assets_page.items.iter().find(|a| a.name == asset_name);

            let asset = match asset_opt {
                Some(a) => a,
                None => return Err(anyhow!("Asset {} not found in release {}", asset_name, tag)),
            };

            let url = asset.browser_download_url.as_ref();

            let resp = client
                .get(url)
                .send()
                .await
                .map_err(|e| anyhow!("failed to GET {}: {}", url, e))?;

            let bytes = resp
                .bytes()
                .await
                .map_err(|e| anyhow!("failed to read bytes: {}", e))?;

            let zip_path = tmp_dir.path().join(&asset_name);
            std::fs::write(&zip_path, &bytes)?;

            // unzip and extract files; look for files whose file_name contains base_name
            let file = File::open(&zip_path)?;
            let mut archive = ZipArchive::new(file)?;
            for i in 0..archive.len() {
                let mut f = archive.by_index(i)?;
                if f.is_dir() {
                    continue;
                }
                let out_name = match Path::new(f.name()).file_name() {
                    Some(n) => n.to_owned(),
                    None => continue,
                };

                let out_path = tmp_dir.path().join(&out_name);
                let mut outfile = File::create(&out_path)?;
                std::io::copy(&mut f, &mut outfile)?;

                if out_name.to_string_lossy().contains(base_name) {
                    set_executable_mode(&out_path)?;
                }
            }
        }

        Ok((tmp_dir, release_version))
    }

    /// Allocates artifacts in the FVM `versions` directory for future use.
    /// Returns the path to the allocated version directory.
    ///
    /// If an artifact with the same name exists in the destination directory,
    /// it will be removed before copying the new artifact.
    async fn store_artifacts(&self, tmp_dir: &TempDir, artifacts: &[String]) -> Result<PathBuf> {
        let version_path = fvm_versions_path()?.join(self.channel.to_string());

        if !version_path.exists() {
            create_dir(&version_path)?;
        }

        for artif in artifacts.iter() {
            let src = tmp_dir.path().join(artif);
            let dst = version_path.join(artif);

            // If the artifact exists in `dst` it should be deleted before copying
            if dst.exists() {
                tracing::debug!(
                    ?dst,
                    "Removing existing artifact in place of upstream artifact"
                );
                remove_file(&dst)?;
            }

            if rename(src.clone(), dst.clone()).is_err() {
                copy(src.clone(), dst.clone()).map_err(|e| {
                    anyhow!(
                        "Error copying artifact {} to {}, {} ",
                        src.display(),
                        dst.display(),
                        e
                    )
                })?;
            }
        }

        Ok(version_path)
    }
}
