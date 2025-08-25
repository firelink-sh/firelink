use anyhow::{Context, Result, anyhow};
use petgraph::algo::toposort;
use petgraph::graph::{Graph, NodeIndex};
use serde::Deserialize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Deserialize, PartialEq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Executor {
    Bash {
        script: String,
        #[serde(default)]
        args: Option<Vec<String>>,
        #[serde(default)]
        env: Option<HashMap<String, String>>,
    },
    Python {
        script: String,
        #[serde(default)]
        args: Option<Vec<String>>,
        #[serde(default)]
        env: Option<HashMap<String, String>>,
    },
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum RunIf {
    AllSuccess,
    AnySuccess,
    Always,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    Cron {
        schedule: String,
    }, // not executed in this minimal example
    Watch {
        source: String,
        #[serde(default)]
        filter: Option<String>,
    }, // not executed in this minimal example
    AfterDeps {
        condition: RunIf,
    },
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct JobConfig {
    pub name: String,
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    pub trigger: Trigger,
    pub executor: Executor,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub jobs: Vec<JobConfig>,
}

impl Config {
    pub fn try_from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let yaml_str = fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&yaml_str)?)
    }

    /// Build a DAG and index maps. Also validates unknown deps and cycles.
    pub fn build_graph(&self) -> Result<SchedulerGraph> {
        let mut graph = Graph::<JobConfig, ()>::new();
        let mut name_to_idx: HashMap<String, NodeIndex> = HashMap::new();

        // nodes
        for job in &self.jobs {
            if name_to_idx.contains_key(&job.name) {
                return Err(anyhow!("duplicate job name '{}'", job.name));
            }
            let idx = graph.add_node(job.clone());
            name_to_idx.insert(job.name.clone(), idx);
        }

        // edges
        for job in &self.jobs {
            if let Some(deps) = &job.depends_on {
                let &job_idx = name_to_idx
                    .get(&job.name)
                    .ok_or_else(|| anyhow!("internal: missing node for {}", job.name))?;
                for dep in deps {
                    let &dep_idx = name_to_idx
                        .get(dep)
                        .ok_or_else(|| anyhow!("unknown dep '{}' for job '{}'", dep, job.name))?;
                    graph.add_edge(dep_idx, job_idx, ());
                }
            }
        }

        // detect cycles early
        toposort(&graph, None).map_err(|cycle| {
            let bad = &graph[cycle.node_id()];
            anyhow!("cycle detected involving job '{}'", bad.name)
        })?;

        Ok(SchedulerGraph { graph, name_to_idx })
    }
}

#[derive(Debug)]
pub struct SchedulerGraph {
    graph: Graph<JobConfig, ()>,
    name_to_idx: HashMap<String, NodeIndex>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobState {
    Pending,
    Running,
    Success,
    Failed,
    Skipped, // not run due to RunIf condition or failed deps
}

#[derive(Debug, Clone)]
struct RuntimeJob {
    idx: NodeIndex,
    cfg: JobConfig,
    state: JobState,
    remaining_deps: usize,
}

#[derive(Debug)]
struct Scheduler {
    dag: SchedulerGraph,
    jobs: Vec<RuntimeJob>,
    idx_to_pos: HashMap<NodeIndex, usize>,
    dependents: HashMap<NodeIndex, Vec<NodeIndex>>,
}

impl Scheduler {
    fn new(dag: SchedulerGraph) -> Self {
        let mut jobs = Vec::new();
        let mut idx_to_pos = HashMap::new();
        // precompute indegrees and dependents
        let mut remaining_deps: HashMap<NodeIndex, usize> = HashMap::new();
        let mut dependents: HashMap<NodeIndex, Vec<NodeIndex>> = HashMap::new();

        for idx in dag.graph.node_indices() {
            remaining_deps.insert(
                idx,
                dag.graph
                    .neighbors_directed(idx, petgraph::Incoming)
                    .count(),
            );
            dependents.entry(idx).or_default();
        }
        for edge in dag.graph.edge_indices() {
            let (from, to) = dag.graph.edge_endpoints(edge).unwrap();
            dependents.entry(from).or_default().push(to);
        }

        for idx in dag.graph.node_indices() {
            let cfg = dag.graph[idx].clone();
            let rd = *remaining_deps.get(&idx).unwrap();
            let pos = jobs.len();
            idx_to_pos.insert(idx, pos);
            jobs.push(RuntimeJob {
                idx,
                cfg,
                state: JobState::Pending,
                remaining_deps: rd,
            });
        }

        Scheduler {
            dag,
            jobs,
            idx_to_pos,
            dependents,
        }
    }

    fn pos(&self, idx: NodeIndex) -> usize {
        *self.idx_to_pos.get(&idx).unwrap()
    }

    fn dependency_statuses(&self, idx: NodeIndex) -> (usize, usize, usize) {
        // returns (success, failed, skipped)
        let mut s = 0;
        let mut f = 0;
        let mut k = 0;
        for dep in self.dag.graph.neighbors_directed(idx, petgraph::Incoming) {
            match self.jobs[self.pos(dep)].state {
                JobState::Success => s += 1,
                JobState::Failed => f += 1,
                JobState::Skipped => k += 1,
                _ => {}
            }
        }
        (s, f, k)
    }

    fn runif_allows(&self, job: &JobConfig, idx: NodeIndex) -> bool {
        match &job.trigger {
            Trigger::AfterDeps { condition } => {
                let (succ, fail, skip) = self.dependency_statuses(idx);
                match condition {
                    RunIf::AllSuccess => {
                        fail == 0
                            && skip == 0
                            && succ
                                == self
                                    .dag
                                    .graph
                                    .neighbors_directed(idx, petgraph::Incoming)
                                    .count()
                    }
                    RunIf::AnySuccess => succ > 0,
                    RunIf::Always => true,
                }
            }
            // For this minimal version, Cron/Watch jobs are not event-driven; if they have deps,
            // we treat them like AfterDeps(Always). If they have no deps they start immediately.
            Trigger::Cron { .. } | Trigger::Watch { .. } => true,
        }
    }

    fn initial_ready(&self) -> Vec<NodeIndex> {
        self.dag
            .graph
            .node_indices()
            .filter(|&idx| self.jobs[self.pos(idx)].remaining_deps == 0)
            .collect()
    }
}

#[derive(Debug)]
enum WorkerMsg {
    Run(NodeIndex),
    Shutdown,
}

#[derive(Debug)]
enum ManagerMsg {
    Finished { idx: NodeIndex, ok: bool },
    Skipped { idx: NodeIndex }, // not run due to run-if condition
}

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "res/example-config2.yaml".to_string());
    let cfg = Config::try_from_yaml_file(&path).context("loading YAML")?;
    let dag = cfg.build_graph().context("building graph")?;
    let mut sched = Scheduler::new(dag);

    let max_workers: usize = 4; // fixed pool size for now

    let (tx_mgr, mut rx_mgr) = mpsc::channel::<ManagerMsg>(1024);
    let (tx_work, rx_work) = mpsc::channel::<WorkerMsg>(1024);

    let jobs_snapshot = sched.jobs.clone();
    let tx_mgr_clone = tx_mgr.clone();
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(max_workers));

    // Worker dispatcher: single consumer, fan out jobs
    tokio::spawn(async move {
        let mut rx_work = rx_work;
        while let Some(msg) = rx_work.recv().await {
            match msg {
                WorkerMsg::Run(idx) => {
                    let permit = semaphore.clone().acquire_owned().await.unwrap();
                    let tx_mgr = tx_mgr_clone.clone();
                    let job = jobs_snapshot
                        .iter()
                        .find(|j| j.idx == idx)
                        .unwrap()
                        .cfg
                        .clone();

                    tokio::spawn(async move {
                        let ok = run_job(&job).await;
                        let _ = tx_mgr.send(ManagerMsg::Finished { idx, ok }).await;
                        drop(permit);
                    });
                }
                WorkerMsg::Shutdown => break,
            }
        }
    });

    // === Trigger handling ===
    let mut ready: VecDeque<NodeIndex> = VecDeque::new();

    for job in &sched.jobs {
        match &job.cfg.trigger {
            Trigger::AfterDeps { .. } => {
                if job.remaining_deps == 0 {
                    ready.push_back(job.idx);
                }
            }
            Trigger::Cron { schedule } => {
                let tx_work = tx_work.clone();
                let idx = job.idx;
                let expr = schedule.clone();
                // spawn cron schedule
                tokio::spawn(async move {
                    use tokio_cron_scheduler::{Job, JobScheduler};
                    let sched = JobScheduler::new().await.unwrap();
                    let j = Job::new(expr.parse::<String>().unwrap(), move |_uuid, _l| {
                        let tx_work = tx_work.clone();
                        let _ = tx_work.try_send(WorkerMsg::Run(idx));
                    })
                    .unwrap();
                    sched.add(j).await.unwrap();
                    sched.start().await.unwrap();
                });
            }
            Trigger::Watch { .. } => {
                // ignored for now
            }
        }
    }

    // Seed after_deps jobs
    dispatch_ready(&mut sched, &mut ready, &tx_work, &tx_mgr).await?;

    // Manager loop (simplified: just waits for job completions)
    while let Some(msg) = rx_mgr.recv().await {
        match msg {
            ManagerMsg::Finished { idx, ok } => {
                let pos = sched.pos(idx);
                sched.jobs[pos].state = if ok {
                    JobState::Success
                } else {
                    JobState::Failed
                };

                // unlock dependents
                if let Some(deps) = sched.dependents.get(&idx) {
                    for &d in deps {
                        let p = sched.pos(d);
                        if sched.jobs[p].remaining_deps > 0 {
                            sched.jobs[p].remaining_deps -= 1;
                        }
                        if sched.jobs[p].remaining_deps == 0 {
                            ready.push_back(d);
                        }
                    }
                }

                dispatch_ready(&mut sched, &mut ready, &tx_work, &tx_mgr).await?;
            }
            ManagerMsg::Skipped { .. } => {}
        }
    }

    Ok(())
}

async fn dispatch_ready(
    sched: &mut Scheduler,
    ready: &mut VecDeque<NodeIndex>,
    tx_work: &Sender<WorkerMsg>,
    tx_mgr: &Sender<ManagerMsg>,
) -> Result<()> {
    while let Some(idx) = ready.pop_front() {
        let pos = sched.pos(idx);
        let job = &sched.jobs[pos];
        if !sched.runif_allows(&job.cfg, idx) {
            let _ = tx_mgr.send(ManagerMsg::Skipped { idx }).await;
            continue;
        }
        tx_work.send(WorkerMsg::Run(idx)).await?;
    }
    Ok(())
}

async fn run_job(job: &JobConfig) -> bool {
    match &job.executor {
        Executor::Bash { script, args, env } => {
            let mut cmd = Command::new("bash");
            cmd.arg(script);
            if let Some(a) = args {
                cmd.args(a);
            }
            if let Some(envs) = env {
                cmd.envs(envs.clone());
            }
            println!("\n============ RUNNING (bash) {}", job.name);
            match cmd.status().await {
                Ok(status) => {
                    println!(
                        "\n============ FINISHED {} with status {}",
                        job.name, status
                    );
                    status.success()
                }
                Err(e) => {
                    eprintln!("× Error running {}: {}", job.name, e);
                    false
                }
            }
        }
        Executor::Python { script, args, env } => {
            let mut cmd = Command::new("python");
            cmd.arg(script);
            if let Some(a) = args {
                cmd.args(a);
            }
            if let Some(envs) = env {
                cmd.envs(envs.clone());
            }
            println!("\n============ RUNNING (python) {}", job.name);
            match cmd.status().await {
                Ok(status) => {
                    println!(
                        "\n============ FINISHED {} with status {}\n",
                        job.name, status
                    );
                    status.success()
                }
                Err(e) => {
                    eprintln!("× Error running {}: {}", job.name, e);
                    false
                }
            }
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
