//! Running-executable identity for user-facing commands and descendants.

use std::path::PathBuf;

use anyhow::{Context, Result};

pub(crate) fn running_invocation_name() -> String {
    #[cfg(test)]
    {
        "nac-web".to_string()
    }
    #[cfg(not(test))]
    {
        std::env::current_exe()
            .ok()
            .and_then(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "nac-web".to_string())
    }
}

pub(crate) fn worker_executable(configured: Option<PathBuf>) -> Result<PathBuf> {
    let running = std::env::current_exe().context("failed to resolve current executable")?;
    resolve_worker_executable(configured, running)
}

fn resolve_worker_executable(
    configured: Option<PathBuf>,
    running_executable: PathBuf,
) -> Result<PathBuf> {
    configured
        .map(canonicalize_file)
        .transpose()
        .map(|configured| configured.unwrap_or(running_executable))
}

fn canonicalize_file(path: PathBuf) -> Result<PathBuf> {
    let resolved = path
        .canonicalize()
        .with_context(|| format!("failed to resolve executable {}", path.display()))?;
    if !resolved.is_file() {
        anyhow::bail!("{} is not a file", resolved.display());
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_descendants_default_to_the_exact_running_executable() {
        let running = PathBuf::from("/tmp/source install with spaces/nac-my-branch");
        assert_eq!(
            resolve_worker_executable(None, running.clone()).unwrap(),
            running
        );
    }
}
