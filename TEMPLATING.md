```rust
use chrono::{DateTime, Utc};
use std::cmp::Ordering;

#[derive(Debug)]
struct Job {
    name: String,
    next_run: DateTime<Utc>,
    interval: chrono::Duration, // For recurring jobs
}

impl PartialEq for Job {
    fn eq(&self, other: &Self) -> bool {
        self.next_run == other.next_run
    }
}

impl Eq for Job {}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(other.next_run.cmp(&self.next_run)) // Reverse for min-heap
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> Ordering {
        other.next_run.cmp(&self.next_run)
    }
}

```

```rust
use std::collections::BinaryHeap;

struct JobQueue {
    heap: BinaryHeap<Job>,
}

impl JobQueue {
    fn new() -> Self {
        JobQueue {
            heap: BinaryHeap::new(),
        }
    }

    fn push(&mut self, job: Job) {
        self.heap.push(job);
    }

    fn pop_next(&mut self) -> Option<Job> {
        self.heap.pop()
    }

    fn peek_next(&self) -> Option<&Job> {
        self.heap.peek()
    }
}

```

```rust
use std::thread;
use std::time::Duration;

fn run_scheduler(mut queue: JobQueue) {
    loop {
        if let Some(next_job) = queue.peek_next() {
            let now = Utc::now();
            let wait_time = (next_job.next_run - now).to_std().unwrap_or(Duration::ZERO);

            if wait_time > Duration::ZERO {
                thread::sleep(wait_time);
            }

            let job = queue.pop_next().unwrap();
            run_job(&job);

            // Reschedule if recurring
            let rescheduled = Job {
                name: job.name.clone(),
                next_run: job.next_run + job.interval,
                interval: job.interval,
            };
            queue.push(rescheduled);
        } else {
            thread::sleep(Duration::from_secs(1)); // Idle sleep
        }
    }
}

fn run_job(job: &Job) {
    println!("Running job: {} at {}", job.name, Utc::now());
    // Here you’d spawn a process, run a script, etc.
}
```

```rust
fn main() {
    let mut queue = JobQueue::new();

    let job_a = Job {
        name: "JobA".to_string(),
        next_run: Utc::now() + chrono::Duration::seconds(10),
        interval: chrono::Duration::days(1),
    };

    queue.push(job_a);

    run_scheduler(queue);
}
```

