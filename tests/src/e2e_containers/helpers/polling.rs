//! Generic polling/retry utilities for E2E tests

use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

use super::E2EError;

/// Configuration for wait operations
pub struct WaitConfig {
    pub max_attempts: u32,
    pub interval: Duration,
    pub description: &'static str,
}

impl Default for WaitConfig {
    fn default() -> Self {
        Self {
            max_attempts: 30,
            interval: Duration::from_secs(2),
            description: "condition",
        }
    }
}

impl WaitConfig {
    /// Create a new config with a custom description
    pub fn with_description(description: &'static str) -> Self {
        Self {
            description,
            ..Default::default()
        }
    }

    /// Set max attempts
    pub fn max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = max;
        self
    }

    /// Set interval between attempts
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}

/// Wait for an async condition to become true
pub async fn wait_for<F, Fut>(config: WaitConfig, condition: F) -> Result<(), E2EError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    for attempt in 1..=config.max_attempts {
        if condition().await {
            tracing::info!("{} ready (attempt {})", config.description, attempt);
            return Ok(());
        }
        if attempt % 5 == 0 {
            tracing::info!(
                "Waiting for {}... (attempt {})",
                config.description,
                attempt
            );
        }
        sleep(config.interval).await;
    }
    Err(E2EError::Timeout(config.description.to_string()))
}

/// Wait for an async condition that returns Option<T>
pub async fn wait_for_value<F, Fut, T>(config: WaitConfig, condition: F) -> Result<T, E2EError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    for attempt in 1..=config.max_attempts {
        if let Some(value) = condition().await {
            tracing::info!("{} found (attempt {})", config.description, attempt);
            return Ok(value);
        }
        if attempt % 5 == 0 {
            tracing::info!(
                "Waiting for {}... (attempt {})",
                config.description,
                attempt
            );
        }
        sleep(config.interval).await;
    }
    Err(E2EError::Timeout(config.description.to_string()))
}

/// Wait for an async condition that returns Result<Option<T>>
pub async fn wait_for_result<F, Fut, T>(config: WaitConfig, condition: F) -> Result<T, E2EError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Option<T>, E2EError>>,
{
    for attempt in 1..=config.max_attempts {
        match condition().await {
            Ok(Some(value)) => {
                tracing::info!("{} found (attempt {})", config.description, attempt);
                return Ok(value);
            }
            Ok(None) => {
                // Condition not met yet, continue polling
            }
            Err(e) => {
                tracing::warn!(
                    "Error checking {}: {} (attempt {})",
                    config.description,
                    e,
                    attempt
                );
                // Continue polling on transient errors
            }
        }
        if attempt % 5 == 0 {
            tracing::info!(
                "Waiting for {}... (attempt {})",
                config.description,
                attempt
            );
        }
        sleep(config.interval).await;
    }
    Err(E2EError::Timeout(config.description.to_string()))
}
