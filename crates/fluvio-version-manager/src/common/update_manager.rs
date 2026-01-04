use std::fs::File;
use std::io::copy;
use std::path::PathBuf;

use anyhow::{bail, Result};
use semver::Version;
use tempfile::TempDir;

use octocrab::Octocrab;
use reqwest::Client;
use zip::ZipArchive;

use crate::common::executable::{remove_fvm_binary_if_exists, set_executable_mode};

use super::notify::Notify;
use super::workdir::fvm_bin_path;
use super::TARGET;

/// Updates Manager for the Fluvio Version Manager
pub struct UpdateManager {
    notify: Notify,
}

impl UpdateManager {
    pub fn new(notify: &Notify) -> Self {
        Self {
            notify: notify.to_owned(),
        }
    }

    pub async fn update(&self, version: &Version) -> Result<()> {
        self.notify.info(format!("Downloading fvm@{version}"));
        let (_tmp_dir, new_fvm_bin) = self.download(version).await?;

        self.notify.info(format!("Installing fvm@{version}"));
        self.install(&new_fvm_bin).await?;
        self.notify
            .done(format!("Installed fvm@{version} with success"));

        Ok(())
    }

    /// Downloads Fluvio Version Manager binary into a temporary directory
    async fn download(&self, version: &Version) -> Result<(TempDir, PathBuf)> {
        let tmp_dir = TempDir::new()?;

        let octocrab = Octocrab::builder().build()?;

        // Attempt to resolve release by tag (try with leading 'v' first)
        let tag_v = format!("v{}", version);
        let release = match octocrab
            .repos("fluvio-community", "fluvio")
            .releases()
            .get_by_tag(&tag_v)
            .await
        {
            Ok(r) => r,
            Err(_) => octocrab
                .repos("fluvio-community", "fluvio")
                .releases()
                .get_by_tag(&version.to_string())
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to find release for tag {}: {}", version, e)
                })?,
        };

        let release_id = release.id;
        let assets_page = octocrab
            .repos("fluvio-community", "fluvio")
            .releases()
            .assets(*release_id)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("failed to list assets for {}: {}", version, e))?;

        let asset_name = format!("fvm-{}.zip", TARGET);

        let asset_opt = assets_page.items.iter().find(|a| a.name == asset_name);

        let asset = match asset_opt {
            Some(a) => a,
            None => {
                return Err(anyhow::anyhow!(
                    "Asset {} not found in release {}",
                    asset_name,
                    version
                ));
            }
        };

        let url = asset.browser_download_url.as_ref();

        let client = Client::new();
        let resp = client
            .get(url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("failed to GET {}: {}", url, e))?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("failed to read bytes: {}", e))?;

        let zip_path = tmp_dir.path().join(&asset_name);
        std::fs::write(&zip_path, &bytes)?;

        // unzip and extract 'fvm' binary
        let file = File::open(&zip_path)?;
        let mut archive = ZipArchive::new(file)?;
        let mut out_path = None;
        for i in 0..archive.len() {
            let mut f = archive.by_index(i)?;
            if f.is_dir() {
                continue;
            }
            let fname = std::path::Path::new(f.name())
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");

            if fname == "fvm" || fname.contains("fvm") {
                let out = tmp_dir.path().join("fvm");
                let mut outfile = File::create(&out)?;
                copy(&mut f, &mut outfile)?;
                set_executable_mode(&out)?;
                out_path = Some(out);
                break;
            }
        }

        let out_path =
            out_path.ok_or_else(|| anyhow::anyhow!("fvm binary not found inside asset"))?;

        // No checksum verification for GitHub assets at the moment

        Ok((tmp_dir, out_path))
    }

    async fn install(&self, new_fvm_bin: &PathBuf) -> Result<()> {
        let old_fvm_bin = fvm_bin_path()?;

        if !new_fvm_bin.exists() {
            tracing::warn!(?new_fvm_bin, "New fvm binary not found. Aborting update.");
            bail!("Failed to update FVM due to missing binary");
        }

        remove_fvm_binary_if_exists()?;

        tracing::warn!(src=?new_fvm_bin, dst=?old_fvm_bin , "Copying new fvm binary");
        std::fs::copy(new_fvm_bin, &old_fvm_bin)?;

        Ok(())
    }
}
