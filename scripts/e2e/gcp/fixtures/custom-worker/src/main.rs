use anyhow::{bail, Context, Result};
use genohype_pool::distributed::{run_worker, TaskDescriptor, WorkerConfig};
use genohype_pool::{async_trait, TaskHandler, TaskResult};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

const REVISION: &str = env!("GENOHYPE_E2E_REVISION");
const BUILD_ID: &str = env!("GENOHYPE_E2E_BUILD_ID");
const BUILD_IDENTITY: &str = env!("GENOHYPE_E2E_BUILD_IDENTITY");
const TASK_SCHEMA_VERSION: u32 = 1;
const MAX_SLEEP_MS: u64 = 30_000;

fn build_version() -> String {
    BUILD_IDENTITY.to_string()
}

fn identity() -> Value {
    json!({
        "architecture": std::env::consts::ARCH,
        "build_id": BUILD_ID,
        "build_identity": BUILD_IDENTITY,
        "build_version": BUILD_IDENTITY,
        "platform": std::env::consts::OS,
        "protocol_version": genohype_pool::distributed::CUSTOM_WORKER_PROTOCOL_VERSION,
        "revision": REVISION,
        "schema_version": 1,
        "task_schema_version": TASK_SCHEMA_VERSION,
    })
}

#[derive(Default)]
struct FixtureHandler;

impl FixtureHandler {
    async fn execute(task: &TaskDescriptor) -> Result<Value> {
        let kind = task
            .payload
            .get("kind")
            .and_then(Value::as_str)
            .context("custom task requires string field 'kind'")?;

        match kind {
            "echo" => Ok(json!({
                "fixture_identity": identity(),
                "kind": "echo",
                "task_id": task.id,
                "value": task.payload.get("value").cloned().unwrap_or(Value::Null),
            })),
            "sha256" => {
                let input = task
                    .payload
                    .get("input")
                    .and_then(Value::as_str)
                    .context("sha256 task requires string field 'input'")?;
                Ok(json!({
                    "kind": "sha256",
                    "task_id": task.id,
                    "sha256": format!("{:x}", Sha256::digest(input.as_bytes())),
                }))
            }
            "sleep_ms" => {
                let milliseconds = task
                    .payload
                    .get("milliseconds")
                    .and_then(Value::as_u64)
                    .context("sleep_ms task requires integer field 'milliseconds'")?;
                if milliseconds > MAX_SLEEP_MS {
                    bail!("sleep_ms exceeds deterministic fixture limit of {MAX_SLEEP_MS}ms");
                }
                tokio::time::sleep(Duration::from_millis(milliseconds)).await;
                Ok(json!({
                    "kind": "sleep_ms",
                    "milliseconds": milliseconds,
                    "task_id": task.id,
                }))
            }
            "fail" => {
                let code = task
                    .payload
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("requested");
                bail!("deterministic fixture failure: {code}")
            }
            other => bail!("unsupported deterministic fixture task kind: {other}"),
        }
    }
}

#[async_trait]
impl TaskHandler for FixtureHandler {
    async fn handle_task(
        &self,
        _payload: &Value,
        tasks: Vec<TaskDescriptor>,
    ) -> Result<TaskResult> {
        let mut results = Vec::with_capacity(tasks.len());
        for task in &tasks {
            match Self::execute(task).await {
                Ok(result) => results.push(result),
                Err(error) => return Ok(TaskResult::failure(error.to_string())),
            }
        }
        Ok(TaskResult::success(
            tasks.len(),
            Some(json!({
                "fixture_identity": identity(),
                "results": results,
            })),
        ))
    }
}

#[derive(Debug, PartialEq)]
struct StartArgs {
    url: String,
    worker_id: String,
    poll_interval_ms: u64,
}

fn parse_start_args(args: &[String]) -> Result<StartArgs> {
    if args.get(0).map(String::as_str) != Some("service")
        || args.get(1).map(String::as_str) != Some("start-worker")
    {
        bail!("expected: service start-worker --url URL --worker-id ID [--poll-interval MS]");
    }

    let mut url = None;
    let mut worker_id = None;
    let mut poll_interval_ms = 2_000;
    let mut poll_interval_seen = false;
    let mut index = 2;
    while index < args.len() {
        let flag = args[index].as_str();
        let value = args
            .get(index + 1)
            .with_context(|| format!("missing value for {flag}"))?;
        match flag {
            "--url" => {
                if url.replace(value.clone()).is_some() {
                    bail!("--url may only be specified once");
                }
            }
            "--worker-id" => {
                if worker_id.replace(value.clone()).is_some() {
                    bail!("--worker-id may only be specified once");
                }
            }
            "--poll-interval" => {
                if poll_interval_seen {
                    bail!("--poll-interval may only be specified once");
                }
                poll_interval_seen = true;
                poll_interval_ms = value
                    .parse::<u64>()
                    .context("--poll-interval must be an integer")?;
                if poll_interval_ms == 0 || poll_interval_ms > 60_000 {
                    bail!("--poll-interval must be between 1 and 60000 milliseconds");
                }
            }
            other => bail!("unknown start-worker flag: {other}"),
        }
        index += 2;
    }

    let url = url.context("--url is required")?;
    let worker_id = worker_id.context("--worker-id is required")?;
    if url.trim().is_empty() {
        bail!("--url must not be empty");
    }
    if worker_id.trim().is_empty() {
        bail!("--worker-id must not be empty");
    }

    Ok(StartArgs {
        url,
        worker_id,
        poll_interval_ms,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [flag] if flag == "--version" => {
            println!("genohype-e2e-custom-worker 0.1.0 ({})", build_version());
            Ok(())
        }
        [flag] if flag == "--identity-json" => {
            println!("{}", serde_json::to_string(&identity())?);
            Ok(())
        }
        _ => {
            let parsed = parse_start_args(&args)?;
            run_worker(
                WorkerConfig {
                    worker_id: parsed.worker_id,
                    coordinator_url: parsed.url,
                    poll_interval_ms: parsed.poll_interval_ms,
                    build_version: Some(build_version()),
                    ..WorkerConfig::default()
                },
                Arc::new(FixtureHandler),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(payload: Value) -> TaskDescriptor {
        TaskDescriptor {
            id: "custom_0".to_string(),
            task_type: "custom".to_string(),
            label: None,
            index: Some(0),
            total: Some(1),
            payload,
            assignment_attempt: None,
            lease_token: None,
        }
    }

    #[test]
    fn parses_generated_service_contract_and_rejects_unknown_flags() {
        let args = [
            "service",
            "start-worker",
            "--url",
            "http://coordinator:3000",
            "--worker-id",
            "pool-worker-0",
            "--poll-interval",
            "25",
        ]
        .map(str::to_string);
        assert_eq!(
            parse_start_args(&args).unwrap(),
            StartArgs {
                url: "http://coordinator:3000".to_string(),
                worker_id: "pool-worker-0".to_string(),
                poll_interval_ms: 25,
            }
        );

        let mut bad = args.to_vec();
        bad.extend(["--repair".to_string(), "true".to_string()]);
        assert!(parse_start_args(&bad).is_err());
    }

    #[test]
    fn identity_is_stable_and_full_revision_bound() {
        assert_eq!(REVISION.len(), 40);
        assert_eq!(
            identity(),
            json!({
                "architecture": std::env::consts::ARCH,
                "build_id": BUILD_ID,
                "build_identity": BUILD_IDENTITY,
                "build_version": BUILD_IDENTITY,
                "platform": std::env::consts::OS,
                "protocol_version": genohype_pool::distributed::CUSTOM_WORKER_PROTOCOL_VERSION,
                "revision": REVISION,
                "schema_version": 1,
                "task_schema_version": TASK_SCHEMA_VERSION,
            })
        );
    }

    #[tokio::test]
    async fn deterministic_quick_tasks_have_canonical_results() {
        let echo = FixtureHandler::execute(&task(json!({"kind":"echo","value":{"b":2,"a":1}})))
            .await
            .unwrap();
        assert_eq!(echo["value"], json!({"a":1,"b":2}));

        let digest = FixtureHandler::execute(&task(json!({"kind":"sha256","input":"genohype"})))
            .await
            .unwrap();
        assert_eq!(
            digest["sha256"],
            "9b8ce151f951e44e366243c6a1a68838e0622e14135eb9caabb415d8d066a48d"
        );
    }

    #[tokio::test]
    async fn sleep_is_bounded_and_failure_is_stable() {
        FixtureHandler::execute(&task(json!({"kind":"sleep_ms","milliseconds":1})))
            .await
            .unwrap();
        assert!(FixtureHandler::execute(&task(json!({
            "kind":"sleep_ms",
            "milliseconds": MAX_SLEEP_MS + 1
        })))
        .await
        .unwrap_err()
        .to_string()
        .contains("exceeds deterministic fixture limit"));
        assert_eq!(
            FixtureHandler::execute(&task(json!({"kind":"fail","code":"e2e"})))
                .await
                .unwrap_err()
                .to_string(),
            "deterministic fixture failure: e2e"
        );
    }
}
