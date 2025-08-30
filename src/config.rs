use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
pub struct Job {
    pub name: String,
    pub depends_on: Option<Vec<String>>,
    pub trigger: Trigger,
    pub executor: Executor,
}
