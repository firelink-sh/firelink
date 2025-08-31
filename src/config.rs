use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunIf {
    AllSuccess,
    AnySuccess,
    Always,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Trigger {
    Cron { schedule: String },
    AfterDeps { condition: RunIf },
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Executor {
    Bash {
        script: String,
        #[serde(default)]
        args: Option<Vec<String>>,
        #[serde(default)]
        env: Option<HashMap<String, String>>,
    },
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct Job {
    pub name: String,
    pub depends_on: Option<Vec<String>>,
    pub trigger: Trigger,
    pub executor: Executor,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub jobs: Vec<Job>,
}

impl Config {
    pub fn from_yaml_str(yaml: &str) -> Self {
        Config::try_from_yaml_str(yaml).unwrap()
    }
    pub fn try_from_yaml_str(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }
}
