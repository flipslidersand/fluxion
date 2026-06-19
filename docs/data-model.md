# Data Model — Fluxion

## コアデータ構造

### ワークフロー定義（YAML から読み込む静的定義）

```rust
struct Workflow {
    id: WorkflowId,       // UUID
    name: String,
    jobs: Vec<JobDefinition>,
}

struct JobDefinition {
    id: JobId,            // YAML のキー名 (e.g. "fetch", "validate")
    component: ComponentRef, // .wasm ファイルへのパス or URL
    dependencies: Vec<JobId>,
    retry_policy: RetryPolicy,
    timeout: Duration,
    permissions: PermissionSet,
    input: Option<serde_json::Value>,
}

struct RetryPolicy {
    max_attempts: u32,    // デフォルト 1（リトライなし）
    backoff: Duration,    // 再試行間隔
}

struct PermissionSet {
    filesystem: FilesystemPermission,
    network: NetworkPermission,
    memory_mb: u64,
}

struct FilesystemPermission {
    read: Vec<PathBuf>,
    write: Vec<PathBuf>,
}

struct NetworkPermission {
    allow: Vec<String>,   // 許可するホスト ("" = 全拒否)
}
```

### 実行状態（Definition と Run を分離）

```rust
struct WorkflowRun {
    run_id: RunId,        // UUID
    workflow_id: WorkflowId,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    status: RunStatus,
    job_runs: Vec<JobRun>,
}

struct JobRun {
    run_id: RunId,        // 親 WorkflowRun の ID
    job_id: JobId,
    attempt: u32,         // 再試行回数（1 から始まる）
    status: JobStatus,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    output_artifact: Option<ArtifactId>,
    error: Option<JobError>,
}

struct JobError {
    kind: ErrorKind,      // Timeout | PermissionDenied | ComponentPanic | ...
    message: String,
    component_stderr: String,
}
```

### アーティファクト（ジョブ間のデータ受け渡し）

```rust
struct Artifact {
    id: ArtifactId,       // UUID
    run_id: RunId,
    job_id: JobId,
    content: Vec<u8>,     // バイナリ（JSON / CSV / Parquet など）
    content_hash: String, // SHA-256（キャッシュキー）
    created_at: DateTime<Utc>,
}
```

## 状態遷移図

### JobStatus

```
PENDING ──→ READY ──→ RUNNING ──→ SUCCEEDED
                          │
                          ├──→ FAILED ──→ (retry) → READY
                          │
                          └──→ CANCELLED

SKIPPED  (依存ジョブが FAILED かつリトライなし)
```

### RunStatus

```
RUNNING ──→ SUCCEEDED  (全ジョブ SUCCEEDED)
        ──→ FAILED     (いずれかのジョブが FAILED で止まった)
        ──→ CANCELLED  (ユーザーがキャンセル)
```

## 永続化スキーマ（SQLite）

```sql
CREATE TABLE workflow_runs (
    run_id      TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE job_runs (
    run_id      TEXT NOT NULL,
    job_id      TEXT NOT NULL,
    attempt     INTEGER NOT NULL DEFAULT 1,
    status      TEXT NOT NULL,
    started_at  TEXT,
    finished_at TEXT,
    artifact_id TEXT,
    error_kind  TEXT,
    error_msg   TEXT,
    PRIMARY KEY (run_id, job_id, attempt)
);

CREATE TABLE artifacts (
    artifact_id  TEXT PRIMARY KEY,
    run_id       TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    content      BLOB NOT NULL,
    content_hash TEXT NOT NULL,
    created_at   TEXT NOT NULL
);
```

## WIT インターフェース定義

```wit
package fluxion:task@0.1.0;

interface processor {
    record task-input {
        content:  list<u8>,
        metadata: list<tuple<string, string>>,
    }

    record task-output {
        content:  list<u8>,
        metadata: list<tuple<string, string>>,
    }

    process: func(input: task-input) -> result<task-output, string>;
}

world task-component {
    export processor;
}
```

全コンポーネントはこの `task-component` world を実装する。
ホストは `process()` を呼び出し、`result<task-output, string>` で成否を受け取る。
