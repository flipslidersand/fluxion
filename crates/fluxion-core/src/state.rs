use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum JobStatus {
    Pending,
    Ready,
    Running,
    Succeeded { elapsed: Duration },
    Failed { elapsed: Duration, reason: String },
    Cancelled,
}

impl JobStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded { .. } | Self::Failed { .. } | Self::Cancelled
        )
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Ready => "READY",
            Self::Running => "RUNNING",
            Self::Succeeded { .. } => "SUCCESS",
            Self::Failed { .. } => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }
}
