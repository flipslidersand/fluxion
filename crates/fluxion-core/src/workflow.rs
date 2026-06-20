use anyhow::{Context, Result};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub name: String,
    pub jobs: IndexMap<String, JobDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDefinition {
    pub component: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub input: Option<String>,
}

impl Workflow {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let src = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read {:?}", path.as_ref()))?;
        let wf: Self =
            serde_yaml::from_str(&src).with_context(|| "Failed to parse workflow YAML")?;
        wf.validate()?;
        Ok(wf)
    }

    fn validate(&self) -> Result<()> {
        for (job_id, def) in &self.jobs {
            for dep in &def.depends_on {
                anyhow::ensure!(
                    self.jobs.contains_key(dep),
                    "Job '{}' depends on unknown job '{}'",
                    job_id,
                    dep
                );
            }
        }
        Ok(())
    }
}
