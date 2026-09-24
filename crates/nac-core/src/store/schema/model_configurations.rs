use super::*;

/// Reusable model settings the launch modal offers by name.
///
/// The secret never lands here: `api_key_env` holds the name the key is filed
/// under in the credential store (the same indirection as `config.toml`). The
/// table is global rather than per-session, hence no foreign key.
pub(super) fn create_model_configurations_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS model_configurations (
             config_id TEXT PRIMARY KEY,
             name TEXT NOT NULL UNIQUE CHECK (length(trim(name)) > 0),
             backend TEXT NOT NULL CHECK (length(trim(backend)) > 0),
             model TEXT NOT NULL CHECK (length(trim(model)) > 0),
             base_url TEXT NOT NULL CHECK (length(trim(base_url)) > 0),
             allow_insecure_http INTEGER NOT NULL DEFAULT 0 CHECK (allow_insecure_http IN (0, 1)),
             api_key_env TEXT,
             reasoning_effort TEXT,
             extra_headers_json TEXT NOT NULL DEFAULT '{{}}',
             orchestrator_compaction_threshold INTEGER CHECK ({THRESHOLD_CHECK}),
             initial_prompt TEXT,
             created_at TEXT NOT NULL,
             updated_at TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_model_configurations_name
             ON model_configurations(name);",
        THRESHOLD_CHECK = threshold_check(),
    ))?;
    ensure_column(
        conn,
        "model_configurations",
        "orchestrator_compaction_threshold",
        &format!("INTEGER CHECK ({})", threshold_check()),
    )?;
    ensure_column(conn, "model_configurations", "initial_prompt", "TEXT")?;
    Ok(())
}

/// Same bound as the session column, so a saved default always materializes.
fn threshold_check() -> String {
    format!(
        "orchestrator_compaction_threshold IS NULL OR \
         (typeof(orchestrator_compaction_threshold) = 'integer' \
          AND orchestrator_compaction_threshold > 0 \
          AND orchestrator_compaction_threshold <= {})",
        crate::MAX_SUPPORTED_TOKEN_COUNT
    )
}
