//! Install Command
//!
//! Downloads and stores the sepecific Fluvio Version binaries in the local
//! FVM cache.

use std::fs::create_dir_all;

use anyhow::Result;
use clap::Parser;

use fluvio_hub_util::fvm::Channel;

use crate::common::notify::Notify;
use crate::common::version_installer::VersionInstaller;
use crate::common::workdir::fvm_versions_path;

/// The `install` command is responsible of installing the desired Package Set
#[derive(Debug, Parser)]
pub struct InstallOpt {
    /// Version to install: stable, latest, or named-version x.y.z
    #[arg(index = 1, default_value_t = Channel::Stable)]
    version: Channel,
}

impl InstallOpt {
    pub async fn process(&self, notify: Notify) -> Result<()> {
        let versions_path = fvm_versions_path()?;

        if !versions_path.exists() {
            tracing::info!(?versions_path, "Creating versions directory");
            create_dir_all(&versions_path)?;
        }

        VersionInstaller::new(self.version.to_owned(), notify)
            .install()
            .await
    }
}
