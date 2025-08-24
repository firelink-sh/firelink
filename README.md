<div align="center">

# firelink
##### A highly efficient, platform agnostic, and lightweight job scheduler.

</div>

## ✨ Features

- ✅ **YAML-based job definitions**
- ✅ **Cron-style scheduling**
- ✅ **Event-based triggers (e.g. S3 bucket watcher)**
- ✅ **Dependency-aware execution (DAG resolution)**
- ✅ **Efficient timer queue architecture**
- ✅ **Minimal CPU usage when idle**
- ✅ **Runs as a long-lived binary daemon**
- ✅ **Pluggable trigger system**

---

## 📦 Example Job Config (`config.yaml`)

```yaml
jobs:
  - name: jobA
    trigger: cron
    schedule: "0 18 * * *"  # Run daily at 18:00

  - name: jobB
    trigger: watch
    source: "s3://my-bucket/input/"

  - name: jobC
    trigger: manual
    depends_on: [jobA, jobB]
```

## Development Roadmap

### Phase 0: Project Setup & Vision

Initialize Rust project (cargo init firelink)

Set up logging (log, env_logger)

Choose crates:
- chrono for time
- serde_yaml for config
- tokio for async runtime
- aws-sdk-s3 for S3 integration
- petgraph or custom DAG for dependencies

### Phase 1: Config Parsing & Job Modeling
- Define Job struct with fields: name, trigger, schedule, depends_on, etc.
- Implement YAML parser with serde_yaml
- Validate config (cycles, missing deps, malformed triggers)
- Build internal DAG of job dependencies

### Phase 2: Timer Queue & Cron Trigger
- Build priority queue for upcoming jobs
- Implement scheduler loop using chrono + std::thread::sleep
- Support recurring jobs
- Execute jobs as subprocesses or closures
- Track job state (pending, running, done, failed)

### Phase 3: S3 Watcher Trigger
- Set up async runtime (tokio)
- Use aws-sdk-s3 to poll or subscribe to bucket events
- Detect new files and trigger associated jobs
- Integrate watcher into scheduler loop

### Phase 4: Dependency Resolution
- Build DAG from job definitions
- Track job completion status
- Ensure jobs run only when all dependencies are complete
- Handle edge cases (failed deps, circular refs)

### Phase 5: Execution Engine & Error Handling
- Run jobs as subprocesses or async tasks
- Capture stdout/stderr and exit codes
- Retry failed jobs (configurable)
- Timeout support
- Log execution history

Phase 6: CLI & Daemonization
- Build CLI with clap or structopt
- Support commands: firelink run config.yaml, firelink status, firelink stop
- Run as background process
- Handle signals (SIGINT, SIGTERM)

Phase 7: Monitoring & Reporting (Optional)
- In-memory or file-based job state tracking
- Optional web dashboard (e.g. with axum)
- Export logs or metrics (Prometheus)
