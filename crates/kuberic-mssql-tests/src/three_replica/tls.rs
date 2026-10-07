use std::path::Path;
use std::time::Duration;

use crate::fixture::process::ProcessRunner;
use crate::fixture::secrets::PrivateFile;
use crate::fixture::tls::{
    MemberTlsAssets, SharedTlsAssets, TlsAssetRecorder, TlsError, TlsGenerationRequest,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsAssets {
    pub ca_certificate: PrivateFile,
    pub ca_private_key: PrivateFile,
    pub members: [MemberTlsAssets; 3],
}

impl TlsAssets {
    pub fn generate(
        root: &Path,
        hostnames: [&str; 3],
        member_data_directories: [&Path; 3],
        timeout: Duration,
        runner: &impl ProcessRunner,
    ) -> Result<Self, TlsError> {
        Self::from_shared(SharedTlsAssets::generate(
            TlsGenerationRequest {
                root,
                hostnames: &hostnames,
                member_data_directories: &member_data_directories,
                ca_common_name: "Kuberic three replica test CA",
                timeout,
            },
            runner,
        )?)
    }

    pub fn generate_recorded(
        root: &Path,
        hostnames: [&str; 3],
        member_data_directories: [&Path; 3],
        timeout: Duration,
        runner: &impl ProcessRunner,
        recorder: &mut impl TlsAssetRecorder,
    ) -> Result<Self, TlsError> {
        Self::from_shared(SharedTlsAssets::generate_recorded(
            TlsGenerationRequest {
                root,
                hostnames: &hostnames,
                member_data_directories: &member_data_directories,
                ca_common_name: "Kuberic three replica test CA",
                timeout,
            },
            runner,
            recorder,
        )?)
    }

    fn from_shared(shared: SharedTlsAssets) -> Result<Self, TlsError> {
        Ok(Self {
            ca_certificate: shared.ca_certificate,
            ca_private_key: shared.ca_private_key,
            members: shared.members.try_into().map_err(|_| TlsError::Helper)?,
        })
    }

    pub fn private_files(&self) -> impl Iterator<Item = &PrivateFile> {
        [
            &self.ca_certificate,
            &self.ca_private_key,
            &self.members[0].server_certificate,
            &self.members[0].server_private_key,
            &self.members[1].server_certificate,
            &self.members[1].server_private_key,
            &self.members[2].server_certificate,
            &self.members[2].server_private_key,
        ]
        .into_iter()
    }
}
