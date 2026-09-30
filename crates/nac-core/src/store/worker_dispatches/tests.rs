use super::*;
use std::io::{BufRead, Write};
use std::process::{Command, Stdio};

fn fixture() -> (PathBuf, WorkerDispatchIdentity) {
    let path = std::env::temp_dir()
        .join(format!("nac-worker-receipt-{}", uuid::Uuid::new_v4()))
        .join("store.db");
    initialize(&path).unwrap();
    insert_test_session(&path, "session");
    let identity =
        admit_worker_dispatch(&path, "session", "worker", "dispatch", None, "action").unwrap();
    (path, identity)
}

#[test]
fn worker_commit_replay_is_exact_and_generation_fenced() {
    let (path, identity) = fixture();
    let id = commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Ok).unwrap();
    assert_eq!(
        commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Ok).unwrap(),
        id
    );
    assert!(commit_worker_episode(&path, &identity, "changed", EpisodeStatus::Ok).is_err());
    assert!(commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Error).is_err());
    for changed in [
        WorkerDispatchIdentity {
            session_id: "other".into(),
            ..identity.clone()
        },
        WorkerDispatchIdentity {
            thread_name: "other".into(),
            ..identity.clone()
        },
        WorkerDispatchIdentity {
            dispatch_id: "other".into(),
            ..identity.clone()
        },
        WorkerDispatchIdentity {
            generation: 2,
            ..identity.clone()
        },
        WorkerDispatchIdentity {
            run_id: Some("other".into()),
            ..identity.clone()
        },
    ] {
        assert!(commit_worker_episode(&path, &changed, "answer", EpisodeStatus::Ok).is_err());
    }
    let next =
        admit_worker_dispatch(&path, "session", "worker", "next", None, "next action").unwrap();
    assert_eq!(next.generation, identity.generation + 1);
    assert!(commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Ok).is_err());
    commit_worker_episode(&path, &next, "new answer", EpisodeStatus::Ok).unwrap();
    assert_eq!(thread_read(&path, "session", "worker").unwrap().len(), 2);
    delete_thread(&path, "session", "worker").unwrap();
    assert!(commit_worker_episode(&path, &next, "new answer", EpisodeStatus::Ok).is_err());
    assert!(thread_read(&path, "session", "worker").unwrap().is_empty());
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn worker_commit_failure_rolls_back_episode_and_receipt() {
    let (path, identity) = fixture();
    let conn = open_runtime_connection(&path).unwrap();
    conn.execute_batch("CREATE TRIGGER reject_receipt BEFORE UPDATE ON worker_dispatches BEGIN SELECT RAISE(ABORT, 'injected receipt failure'); END;").unwrap();
    assert!(commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Ok).is_err());
    assert!(!worker_dispatch_committed(&path, "dispatch").unwrap());
    assert!(thread_dispatches(&path, "session", "worker")
        .unwrap()
        .is_empty());
    conn.execute_batch("DROP TRIGGER reject_receipt").unwrap();
    commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Ok).unwrap();
    assert_eq!(thread_read(&path, "session", "worker").unwrap().len(), 1);
    drop(conn);
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn worker_pending_commit_rejects_replaced_or_terminal_session_run() {
    let (path, identity) = fixture();
    let conn = open_runtime_connection(&path).unwrap();
    conn.execute("INSERT INTO thread_events (id, session_id, thread_name, event_json, created_at) VALUES (1, 'session', '__orchestrator__', '{}', 'now')", []).unwrap();
    conn.execute("INSERT INTO session_run_recovery (session_id, run_id, submitted_message_id, status) VALUES ('session', 'new-run', 1, 'active')", []).unwrap();
    assert!(commit_worker_episode(&path, &identity, "stale", EpisodeStatus::Ok).is_err());
    let new = admit_worker_dispatch(
        &path,
        "session",
        "worker",
        "next",
        Some("new-run"),
        "action",
    )
    .unwrap();
    conn.execute(
        "UPDATE session_run_recovery SET status = 'interrupted' WHERE session_id = 'session'",
        [],
    )
    .unwrap();
    assert!(commit_worker_episode(&path, &new, "stale", EpisodeStatus::Ok).is_err());
    assert!(thread_dispatches(&path, "session", "worker")
        .unwrap()
        .is_empty());
    drop(conn);
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn worker_receipt_crash_helper() {
    let Ok(path) = std::env::var("NAC_WORKER_RECEIPT_CRASH_PATH") else {
        return;
    };
    let phase = std::env::var("NAC_WORKER_RECEIPT_CRASH_PHASE").unwrap();
    let path = PathBuf::from(path);
    let identity = WorkerDispatchIdentity {
        session_id: "session".into(),
        thread_name: "worker".into(),
        dispatch_id: "dispatch".into(),
        generation: 1,
        run_id: None,
    };
    let mut conn = open_runtime_connection(&path).unwrap();
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    if phase != "before_send" {
        commit_in_tx(&tx, &identity, "answer", EpisodeStatus::Ok).unwrap();
    }
    if phase == "before_commit" {
        println!("BARRIER");
        std::io::stdout().flush().unwrap();
        let mut control = String::new();
        std::io::stdin().read_line(&mut control).unwrap();
        unreachable!("parent kills at uncommitted transaction barrier");
    }
    tx.commit().unwrap();
    if phase == "after_ack" {
        let ack = crate::worker_protocol::CommitAck {
            session_id: identity.session_id.clone(),
            thread_name: identity.thread_name.clone(),
            dispatch_id: identity.dispatch_id.clone(),
            episode_id: 1,
        };
        println!(
            "{}{}",
            crate::worker_protocol::ACK_PREFIX,
            serde_json::to_string(&ack).unwrap()
        );
    }
    println!("BARRIER");
    std::io::stdout().flush().unwrap();
    let mut control = String::new();
    std::io::stdin().read_line(&mut control).unwrap();
}

#[test]
fn worker_host_crash_restart_distinguishes_pending_from_ack_loss() {
    for phase in ["before_send", "before_commit", "before_ack", "after_ack"] {
        let (path, identity) = fixture();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::worker_dispatches::tests::worker_receipt_crash_helper",
                "--nocapture",
            ])
            .env("NAC_WORKER_RECEIPT_CRASH_PATH", &path)
            .env("NAC_WORKER_RECEIPT_CRASH_PHASE", phase)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        let mut acknowledged = false;
        loop {
            line.clear();
            assert!(
                output.read_line(&mut line).unwrap() > 0,
                "helper failed before {phase}"
            );
            if line.starts_with(crate::worker_protocol::ACK_PREFIX) {
                let expected = crate::worker_protocol::Completion {
                    session_id: "session".into(),
                    thread_name: "worker".into(),
                    dispatch_id: "dispatch".into(),
                    content: "answer".into(),
                };
                assert!(expected.validates_ack(line.trim_end()));
                acknowledged = true;
            }
            if line.trim() == "BARRIER" {
                break;
            }
        }
        assert_eq!(acknowledged, phase == "after_ack");
        child.kill().unwrap();
        child.wait().unwrap();
        initialize(&path).unwrap();
        reconcile_active_run(&path, "session").unwrap();
        reconcile_active_run(&path, "session").unwrap();
        let episodes = thread_dispatches(&path, "session", "worker").unwrap();
        assert_eq!(
            episodes.len(),
            1,
            "{phase}: exactly one recovered terminal episode"
        );
        if matches!(phase, "before_ack" | "after_ack") {
            assert_eq!(episodes[0].status, "ok");
            assert_eq!(
                commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Ok).unwrap(),
                episodes[0].id
            );
        } else {
            assert_eq!(episodes[0].status, "error");
            assert!(commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Ok).is_err());
        }
        let conn = open_runtime_connection(&path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(conn);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}

#[test]
fn worker_receipt_migration_preserves_previous_episode_history() {
    let (path, _) = fixture();
    append_episode(&path, "session", "old", "legacy", "retained").unwrap();
    let conn = open_runtime_connection(&path).unwrap();
    conn.execute_batch("DROP TABLE worker_dispatches; PRAGMA user_version=29;")
        .unwrap();
    drop(conn);
    initialize(&path).unwrap();
    initialize(&path).unwrap();
    assert_eq!(
        thread_read(&path, "session", "old").unwrap()[0].content,
        "retained"
    );
    let identity = admit_worker_dispatch(&path, "session", "new", "new", None, "new").unwrap();
    commit_worker_episode(&path, &identity, "answer", EpisodeStatus::Ok).unwrap();
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn worker_context_requires_host_initialized_schema_without_creating_or_migrating() {
    let root = std::env::temp_dir().join(format!("nac-worker-read-{}", uuid::Uuid::new_v4()));
    let path = root.join("store.db");
    assert!(load_worker_context(&path, "session", "worker", &[]).is_err());
    assert!(
        !root.exists(),
        "worker read cannot initialize a missing store"
    );
    initialize(&path).unwrap();
    insert_test_session(&path, "session");
    let conn = open_runtime_connection(&path).unwrap();
    conn.pragma_update(None, "user_version", schema_version() - 1)
        .unwrap();
    assert!(load_worker_context(&path, "session", "worker", &[]).is_err());
    assert_eq!(
        conn.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        schema_version() - 1
    );
    drop(conn);
    let _ = std::fs::remove_dir_all(root);
}
