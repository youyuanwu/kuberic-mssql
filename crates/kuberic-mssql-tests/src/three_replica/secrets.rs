use std::path::Path;

use crate::fixture::secrets::{PrivateFile, SecretError, SharedCredentialFiles};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialFiles {
    pub sa_username: PrivateFile,
    pub sa_password: PrivateFile,
    pub admin_username: PrivateFile,
    pub admin_password: PrivateFile,
    pub observer_username: PrivateFile,
    pub observer_password: PrivateFile,
    pub endpoint_master_key_passwords: [PrivateFile; 3],
}

impl CredentialFiles {
    pub fn generate(root: &Path, run_id: &str) -> Result<Self, SecretError> {
        let shared = SharedCredentialFiles::generate(root, run_id, 3)?;
        Ok(Self {
            sa_username: shared.sa_username,
            sa_password: shared.sa_password,
            admin_username: shared.admin_username,
            admin_password: shared.admin_password,
            observer_username: shared.observer_username,
            observer_password: shared.observer_password,
            endpoint_master_key_passwords: shared
                .endpoint_master_key_passwords
                .try_into()
                .map_err(|_| SecretError::Io)?,
        })
    }

    pub fn all(&self) -> impl Iterator<Item = &PrivateFile> {
        [
            &self.sa_username,
            &self.sa_password,
            &self.admin_username,
            &self.admin_password,
            &self.observer_username,
            &self.observer_password,
        ]
        .into_iter()
        .chain(self.endpoint_master_key_passwords.iter())
    }
}
