//! Updates version of the current channel to the most recent one

use anyhow::Result;
use clap::Args;

use fluvio_hub_util::fvm::Channel;

use crate::common::notify::Notify;
use crate::common::settings::Settings;
use crate::common::version_installer::VersionInstaller;

#[derive(Debug, Args)]
pub struct UpdateOpt;

impl UpdateOpt {
    pub async fn process(self, notify: Notify) -> Result<()> {
        let settings = Settings::open()?;
        let Some(channel) = settings.channel else {
            notify.info("No channel set, please set a channel first using `fvm switch`");
            return Ok(());
        };

        if channel.is_version_tag() {
            // Abort early if the user is not using a Channel and instead has
            // a static tag set as active
            notify.info("Cannot update a static version tag. You must use a channel.");
            return Ok(());
        }

        match channel {
            Channel::Stable => {
                return VersionInstaller::new(channel, notify).install().await;
            }
            Channel::Latest => {
                return VersionInstaller::new(channel, notify).install().await;
            }
            Channel::Tag(_) | Channel::Other(_) => {
                notify.warn("Static tags cannot be updated. No changes made.");
            }
        }

        Ok(())
    }
}
