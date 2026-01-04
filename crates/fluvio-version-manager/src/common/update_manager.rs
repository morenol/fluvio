use std::fs::File;
use std::io::copy;
use std::path::PathBuf;

use anyhow::{bail, Result};
use semver::Version;
use tempfile::TempDir;

use fluvio_artifacts_util::get_package_noauth;

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
        let binary_data = get_package_noauth("fvm", &version.to_string(), TARGET).await?;
        let out_path = tmp_dir.path().join("fvm");
        let mut file = File::create(&out_path)?;

        copy(&mut binary_data.as_slice(), &mut file)?;
        set_executable_mode(&out_path)?;

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
