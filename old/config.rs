use petgraph::algo::toposort;
use petgraph::graph::{Graph, NodeIndex};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

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
    Python {
        script: String,
        #[serde(default)]
        args: Option<Vec<String>>,
        #[serde(default)]
        env: Option<HashMap<String, String>>,
    },
}

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
    Cron {
        schedule: String,
    },
    Watch {
        source: String,
        #[serde(default)]
        filter: Option<String>,
    },
    AfterDeps {
        condition: RunIf,
    },
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct JobConfig {
    pub name: String,
    pub depends_on: Option<Vec<String>>,
    pub trigger: Trigger,
    pub executor: Executor,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub jobs: Vec<JobConfig>,
}

impl Config {
    pub fn from_yaml_str(yaml: &str) -> Self {
        Config::try_from_yaml_str(yaml).unwrap()
    }

    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Self {
        Config::try_from_yaml_file(path).unwrap()
    }

    pub fn try_from_yaml_str(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn try_from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let yaml_str = fs::read_to_string(path)?;
        let config = serde_yaml::from_str(&yaml_str)?;
        Ok(config)
    }

    pub fn execution_order(&self) -> Result<Vec<&JobConfig>, String> {
        let mut graph = Graph::<&JobConfig, ()>::new();
        let mut indices = HashMap::new();

        for job in &self.jobs {
            let idx = graph.add_node(job);
            indices.insert(job.name.clone(), idx);
        }

        for job in &self.jobs {
            if let Some(deps) = &job.depends_on {
                let &job_idx = indices.get(&job.name).unwrap();
                for dep in deps {
                    if let Some(&dep_idx) = indices.get(dep) {
                        graph.add_edge(dep_idx, job_idx, ());
                    } else {
                        return Err(format!("unknown dep '{}' for job '{}'", dep, job.name));
                    }
                }
            }
        }

        let order = toposort(&graph, None).map_err(|cycle| {
            let bad = &graph[cycle.node_id()];
            format!("cycle detected involving job '{}'", bad.name)
        })?;

        Ok(order.into_iter().map(|idx| graph[idx]).collect())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Pending,
    Running,
    Success,
    Failed,
    Skipped,
}

mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn from_yaml_str_panic() {
        let yaml = r#"
jobs:
- name: job_A
  trigger:
    type: cron
    schedule: "* * * * *"
  executor:
    type: bash
    script: "yea.sh"

- name: job_B
  trigger:
    type: watch
    filter: "*.tar.gz"
  executor:
    type: python
    script: "yea.py"

- name: job_C
  depends_on: [ job_A, job_B ]
  trigger:
    type: after_deps
  executor:
    type: python
    script: "yea_C.py"
"#;
        let _ = Config::from_yaml_str(yaml);
    }

    #[test]
    fn from_yaml_str_ok() {
        let yaml = r#"
jobs:
- name: job_A
  trigger:
    type: cron
    schedule: "* * * * *"
  executor:
    type: python
    script: "path/to/script.py"
    args: [ "--input", "data.csv" ]
    env:
      MY_ENV_VAR: "value"

- name: job_B
  trigger:
    type: watch
    source: "s3://bucket/input"
    filter: "*.tar.gz"
  executor:
    type: bash
    script: "xd.sh"

- name: job_C
  depends_on: [ job_A, job_B ]
  trigger:
    type: after_deps
    condition: all_success
  executor:
    type: bash
    script: "/yo/opt/yooho.sh"
    env:
      AAAAAAAAAAA: "BBBBBBBBBB"
"#;
        let config = Config::from_yaml_str(yaml);
        assert_eq!(config.jobs.len(), 3);

        let mut hm = HashMap::new();
        hm.insert("MY_ENV_VAR".to_string(), "value".to_string());

        assert_eq!(
            config.jobs[0],
            JobConfig {
                name: "job_A".to_string(),
                depends_on: None,
                trigger: Trigger::Cron {
                    schedule: "* * * * *".to_string()
                },
                executor: Executor::Python {
                    script: "path/to/script.py".to_string(),
                    args: Some(vec!["--input".to_string(), "data.csv".to_string()]),
                    env: Some(hm),
                },
            },
        );
    }
}
