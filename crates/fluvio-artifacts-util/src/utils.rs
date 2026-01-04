use std::fs::File;
use std::path::PathBuf;
use std::io::copy;
use std::str::FromStr;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use fluvio_hub_protocol::{PackageMeta, Result, HubError};
use fluvio_hub_protocol::constants::HUB_PACKAGE_EXT;

use crate::fvm::Channel;
use crate::htclient;

const GITHUB_RELEASE_BASE: &str = "https://github.com/fluvio-community/fluvio/releases/download";

/// Used by hub server web api and cli exchange package lists
#[derive(Serialize, Deserialize)]
pub struct PackageList {
    pub packages: Vec<String>,
}

/// Used by hub server web api and cli exchange package lists
#[derive(Serialize, Deserialize)]
pub struct PackageListMeta {
    pub packages: Vec<PackageMeta>,
}

// returns (org, pname, ver)
pub fn cli_pkgname_split(pkgname: &str) -> Result<(&str, &str, &str)> {
    let idx1 = pkgname
        .rfind('@')
        .ok_or_else(|| HubError::InvalidPackageName(format!("{pkgname} missing version")))?;
    let split1 = pkgname.split_at(idx1); // this gives us (pkgname, ver)
    let (orgpkg, verstr) = split1;
    let ver = verstr.trim_start_matches('@');

    let idx2 = orgpkg.find('/').unwrap_or(0);
    let (org, pkgstr) = orgpkg.split_at(idx2);
    let pkg = pkgstr.trim_start_matches('/');

    Ok((org, pkg, ver))
}

// deprecated, but keep for reference for a bit
pub async fn get_package_noauth(bin_name: &str, channel: &str, systuple: &str) -> Result<Vec<u8>> {
    let channel = Channel::from_str(channel)
        .map_err(|e| HubError::General(format!("channel parse error {e}")))?;
    let pkgurl = build_binary_url(bin_name, &channel, systuple);
    let resp = htclient::get(pkgurl)
        .await
        .map_err(|_| HubError::PackageDownload("".into()))?;

    let status = http::StatusCode::from_u16(resp.status().as_u16())
        .map_err(|e| HubError::General(format!("status mapping error {e}")))?;
    match status {
        StatusCode::OK => {}
        _ => {
            return Err(HubError::PackageDownload("".into()));
        }
    }

    let data_zip = resp.body();

    let reader = std::io::Cursor::new(data_zip);
    let mut zip = zip::ZipArchive::new(reader)
        .map_err(|e| HubError::General(format!("unzipping package error {e}")))?;
    let mut file = zip
        .by_index(0)
        .map_err(|e| HubError::General(format!("reading zip file error {e}")))?;
    let mut binary_data = Vec::new();
    copy(&mut file, &mut binary_data)
        .map_err(|e| HubError::General(format!("copying binary data error {e}")))?;

    Ok(binary_data)
}

fn build_binary_url(bin_name: &str, channel: &Channel, systuple: &str) -> String {
    format!(
        "{base}/v{channel}/{bin_name}-{systuple}.zip",
        base = GITHUB_RELEASE_BASE,
    )
}

/// non validating function to make canonical filenames from
/// org pkg version triples
pub fn make_filename(org: &str, pkg: &str, ver: &str) -> String {
    if org.is_empty() {
        format!("{pkg}-{ver}.{HUB_PACKAGE_EXT}")
    } else {
        format!("{org}-{pkg}-{ver}.{HUB_PACKAGE_EXT}")
    }
}

/// Generates Sha256 checksum for a given file
pub fn sha256_digest(path: &PathBuf) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut file = File::open(path)?;

    copy(&mut file, &mut hasher)?;

    let hash_bytes = hasher.finalize();

    Ok(hex::encode(hash_bytes))
}

#[cfg(test)]
mod util_tests {
    use tempfile::TempDir;

    use crate::sha256_digest;

    use super::cli_pkgname_split;

    #[test]
    fn cli_pkgname_split_t() {
        let recs_good = vec![
            ("example@0.0.1", ("", "example", "0.0.1")),
            ("infinyon/example@0.0.1", ("infinyon", "example", "0.0.1")),
        ];
        for rec in recs_good {
            let out = cli_pkgname_split(rec.0);
            assert!(out.is_ok());
            let (org, pkg, ver) = out.unwrap();

            assert_eq!(rec.1.0, org);
            assert_eq!(rec.1.1, pkg);
            assert_eq!(rec.1.2, ver);
        }
    }

    #[test]
    fn creates_shasum_digest() {
        use std::fs::write;

        let tempdir = TempDir::new().unwrap().into_path().to_path_buf();
        let foo_path = tempdir.join("foo");

        write(&foo_path, "foo").unwrap();

        let foo_a_checksum = sha256_digest(&foo_path).unwrap();

        assert_eq!(
            foo_a_checksum,
            "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
        );
    }

    #[test]
    fn checks_files_checksums_diff() {
        use std::fs::write;

        let tempdir = TempDir::new().unwrap().into_path().to_path_buf();
        let foo_path = tempdir.join("foo");
        let bar_path = tempdir.join("bar");

        write(&foo_path, "foo").unwrap();
        write(&bar_path, "bar").unwrap();

        let foo_checksum = sha256_digest(&foo_path).unwrap();
        let bar_checksum = sha256_digest(&bar_path).unwrap();

        assert_ne!(foo_checksum, bar_checksum);
    }

    #[test]
    fn checks_files_checksums_same() {
        use std::fs::write;

        let tempdir = TempDir::new().unwrap().into_path().to_path_buf();
        let foo_path = tempdir.join("foo");

        write(&foo_path, "foo").unwrap();

        let foo_a_checksum = sha256_digest(&foo_path).unwrap();
        let foo_b_checksum = sha256_digest(&foo_path).unwrap();

        assert_eq!(foo_a_checksum, foo_b_checksum);
    }
}
