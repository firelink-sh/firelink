use futures::{SinkExt, StreamExt};
use serde::Serialize;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc};
use warp::Filter;
use warp::ws::{Message, WebSocket};

// --- JobNode for UI ---
#[derive(Serialize, Clone)]
struct JobNode {
    pub name: String,
    pub state: String,
    pub deps: Vec<String>,
}

// --- Worker messages ---
enum WorkerMsg {
    Run(usize),
    Shutdown,
}

// --- Manager messages ---
enum ManagerMsg {
    Finished { idx: usize, ok: bool },
    Skipped { idx: usize },
}

// --- Job structs ---
#[derive(Clone)]
struct JobConfig {
    pub name: String,
    pub depends_on: Vec<String>,
    // triggers & executor omitted for simplicity
}
struct Job {
    pub idx: usize,
    pub cfg: JobConfig,
    pub state: String,
    pub remaining_deps: usize,
}

// --- Scheduler ---
struct Scheduler {
    jobs: Vec<Job>,
}
impl Scheduler {
    fn pos(&self, idx: usize) -> usize {
        self.jobs.iter().position(|j| j.idx == idx).unwrap()
    }
}

// --- Job execution stub ---
async fn run_job(job: &JobConfig) -> bool {
    println!("Running job {}", job.name);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    true
}

// --- Broadcast DAG snapshot ---
async fn broadcast_jobs(tx: &broadcast::Sender<Vec<JobNode>>, sched: &Scheduler) {
    let jobs_snapshot: Vec<JobNode> = sched
        .jobs
        .iter()
        .map(|j| JobNode {
            name: j.cfg.name.clone(),
            state: j.state.clone(),
            deps: j.cfg.depends_on.clone(),
        })
        .collect();
    let _ = tx.send(jobs_snapshot);
}

// --- Main ---
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // --- 1. Initialize scheduler ---
    let mut jobs_list = vec![
        Job {
            idx: 0,
            cfg: JobConfig {
                name: "job_A".to_string(),
                depends_on: vec![],
            },
            state: "Pending".to_string(),
            remaining_deps: 0,
        },
        Job {
            idx: 1,
            cfg: JobConfig {
                name: "job_B".to_string(),
                depends_on: vec!["job_A".to_string()],
            },
            state: "Pending".to_string(),
            remaining_deps: 1,
        },
        Job {
            idx: 2,
            cfg: JobConfig {
                name: "job_C".to_string(),
                depends_on: vec!["job_B".to_string()],
            },
            state: "Pending".to_string(),
            remaining_deps: 1,
        },
    ];
    let sched = Scheduler { jobs: jobs_list };
    let sched_arc = Arc::new(Mutex::new(sched));

    // --- 2. Channels ---
    let (tx_mgr, mut rx_mgr) = mpsc::channel::<ManagerMsg>(1024);
    let (tx_work, mut rx_work) = mpsc::channel::<WorkerMsg>(1024);
    let (tx_ws, _) = broadcast::channel::<Vec<JobNode>>(1024);
    let semaphore = Arc::new(Semaphore::new(4));

    // --- 3. Worker dispatcher ---
    {
        let sched_clone = sched_arc.clone();
        let tx_mgr_clone = tx_mgr.clone();
        let semaphore_clone = semaphore.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx_work.recv().await {
                match msg {
                    WorkerMsg::Run(idx) => {
                        let permit = semaphore_clone.clone().acquire_owned().await.unwrap();
                        let tx_mgr = tx_mgr_clone.clone();
                        let sched = sched_clone.clone();
                        tokio::spawn(async move {
                            let job_cfg = {
                                let s = sched.lock().await;
                                s.jobs[s.pos(idx)].cfg.clone()
                            };
                            let ok = run_job(&job_cfg).await;

                            // update state
                            {
                                let mut s = sched.lock().await;
                                s.jobs[s.pos(idx)].state = if ok {
                                    "Success".into()
                                } else {
                                    "Failed".into()
                                };
                            }

                            let _ = tx_mgr.send(ManagerMsg::Finished { idx, ok }).await;

                            drop(permit);
                        });
                    }
                    WorkerMsg::Shutdown => break,
                }
            }
        });
    }

    // --- 4. Dispatch initial after_deps jobs ---
    {
        let sched_clone = sched_arc.clone();
        let tx_work_clone = tx_work.clone();
        tokio::spawn(async move {
            let mut ready = VecDeque::new();
            let s = sched_clone.lock().await;
            for job in &s.jobs {
                if job.remaining_deps == 0 {
                    ready.push_back(job.idx);
                }
            }
            drop(s);

            while let Some(idx) = ready.pop_front() {
                tx_work_clone.send(WorkerMsg::Run(idx)).await.unwrap();
            }
        });
    }

    // --- 5. Manager loop ---
    {
        let sched_clone = sched_arc.clone();
        let tx_ws_clone = tx_ws.clone();
        let tx_work_clone = tx_work.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx_mgr.recv().await {
                match msg {
                    ManagerMsg::Finished { idx, ok: _ } => {
                        let mut s = sched_clone.lock().await;
                        // unlock dependents
                        for job in &mut s.jobs {
                            if job.cfg.depends_on.contains(&s.jobs[s.pos(idx)].cfg.name) {
                                if job.remaining_deps > 0 {
                                    job.remaining_deps -= 1;
                                }
                                if job.remaining_deps == 0 {
                                    tx_work_clone.send(WorkerMsg::Run(job.idx)).await.unwrap();
                                }
                            }
                        }
                        broadcast_jobs(&tx_ws_clone, &s).await;
                    }
                    ManagerMsg::Skipped { idx: _ } => {}
                }
            }
        });
    }

    // --- 6. Warp server ---
    // WebSocket
    let ws_tx_clone = tx_ws.clone();
    let ws_route = warp::path("ws")
        .and(warp::ws())
        .map(move |ws: warp::ws::Ws| {
            let ws_tx = ws_tx_clone.clone();
            ws.on_upgrade(move |socket| async move {
                let (mut ws_tx_sink, mut ws_rx_stream) = socket.split();
                let mut rx = ws_tx.subscribe();

                // optional read messages from client
                tokio::spawn(
                    async move { while let Some(Ok(_msg)) = ws_rx_stream.next().await {} },
                );

                while let Ok(jobs) = rx.recv().await {
                    let msg = serde_json::to_string(&jobs).unwrap();
                    if ws_tx_sink.send(Message::text(msg)).await.is_err() {
                        break;
                    }
                }
            })
        });

    // Serve index.html
    let index_route = warp::path::end().and(warp::fs::file("static/index.html"));

    let routes = index_route.or(ws_route);

    println!("Server running at http://127.0.0.1:3030");
    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;

    Ok(())
}
