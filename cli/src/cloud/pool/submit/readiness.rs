use crate::distributed::message::DashboardWorker;
use crate::{HailError, Result};
use std::collections::{BTreeMap, BTreeSet};

use super::WORKER_FRESHNESS_SECS;

/// Require the coordinator's fresh registry to describe exactly the managed,
/// running worker VM set. This is deliberately read-only: submit never repairs
/// a failed readiness check.
pub(super) fn require_worker_readiness(
    expected_worker_names: &[String],
    registrations: &[DashboardWorker],
    expected_build: Option<&str>,
) -> Result<()> {
    let expected: BTreeSet<_> = expected_worker_names.iter().cloned().collect();
    let by_id: BTreeMap<_, _> = registrations
        .iter()
        .map(|worker| (worker.worker_id.clone(), worker))
        .collect();

    let mut missing = Vec::new();
    let mut stale = Vec::new();
    let mut invalid_freshness = Vec::new();
    let mut unhealthy = Vec::new();
    let mut missing_build = Vec::new();
    let mut mismatched_build = Vec::new();
    let mut observed_builds = BTreeSet::new();

    for name in &expected {
        let Some(worker) = by_id.get(name) else {
            missing.push(name.clone());
            continue;
        };
        if !worker.last_seen_secs.is_finite() || worker.last_seen_secs < 0.0 {
            invalid_freshness.push(format!("{} ({})", name, worker.last_seen_secs));
        } else if worker.last_seen_secs > WORKER_FRESHNESS_SECS {
            stale.push(format!("{} ({:.1}s)", name, worker.last_seen_secs));
        }
        if !matches!(worker.status.as_str(), "active" | "idle") {
            unhealthy.push(format!("{} ({})", name, worker.status));
        }
        match worker
            .build_version
            .as_deref()
            .filter(|build| !build.trim().is_empty())
        {
            Some(running_build) => {
                observed_builds.insert(running_build.to_string());
                if let Some(expected_build) = expected_build {
                    if running_build != expected_build {
                        mismatched_build.push(format!(
                            "{} (expected {}, running {})",
                            name, expected_build, running_build
                        ));
                    }
                }
            }
            None => missing_build.push(name.clone()),
        }
    }

    // Custom workers may intentionally differ from the stock coordinator. When
    // no durable expected build is available, still fail closed on a mixed fleet.
    if expected_build.is_none() && observed_builds.len() > 1 {
        mismatched_build.push(format!(
            "managed workers report mixed builds: {}",
            observed_builds.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }

    // The coordinator registry must describe exactly the managed VM set. A
    // stale or unhealthy extra registration is still evidence that durable
    // registry state and cloud inventory disagree, so submit fails closed.
    let unexpected: Vec<_> = registrations
        .iter()
        .filter(|worker| !expected.contains(&worker.worker_id))
        .map(|worker| worker.worker_id.clone())
        .collect();
    let duplicate_registrations: Vec<_> = by_id
        .keys()
        .filter(|id| {
            registrations
                .iter()
                .filter(|worker| &worker.worker_id == *id)
                .count()
                > 1
        })
        .cloned()
        .collect();

    if missing.is_empty()
        && stale.is_empty()
        && invalid_freshness.is_empty()
        && unhealthy.is_empty()
        && unexpected.is_empty()
        && missing_build.is_empty()
        && mismatched_build.is_empty()
        && duplicate_registrations.is_empty()
    {
        return Ok(());
    }

    let mut details = Vec::new();
    if !missing.is_empty() {
        details.push(format!("missing registrations: {}", missing.join(", ")));
    }
    if !stale.is_empty() {
        details.push(format!("stale registrations: {}", stale.join(", ")));
    }
    if !invalid_freshness.is_empty() {
        details.push(format!(
            "invalid heartbeat ages: {}",
            invalid_freshness.join(", ")
        ));
    }
    if !unhealthy.is_empty() {
        details.push(format!("unready registrations: {}", unhealthy.join(", ")));
    }
    if !unexpected.is_empty() {
        details.push(format!(
            "unexpected registrations: {}",
            unexpected.join(", ")
        ));
    }
    if !duplicate_registrations.is_empty() {
        details.push(format!(
            "duplicate registrations: {}",
            duplicate_registrations.join(", ")
        ));
    }
    if !missing_build.is_empty() {
        details.push(format!(
            "missing build identity: {}",
            missing_build.join(", ")
        ));
    }
    if !mismatched_build.is_empty() {
        details.push(format!("build mismatches: {}", mismatched_build.join(", ")));
    }

    Err(HailError::Io(std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        format!(
            "Worker readiness check failed: {}. Submit did not mutate the pool. \
             Repair explicitly with 'genohype pool update-binary' or scale the pool, then retry.",
            details.join("; ")
        ),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(id: &str, status: &str, age: f64, build: Option<&str>) -> DashboardWorker {
        DashboardWorker {
            worker_id: id.into(),
            status: status.into(),
            current_batch_size: None,
            max_batch_capacity: None,
            last_seen_secs: age,
            telemetry: None,
            total_items: 0,
            tasks_completed: 0,
            current_task: None,
            build_version: build.map(str::to_string),
            effective_status: None,
        }
    }

    #[test]
    fn exact_fresh_fleet_is_ready() {
        let expected = vec!["pool-worker-0".to_string()];
        assert!(require_worker_readiness(
            &expected,
            &[worker("pool-worker-0", "idle", 1.0, Some("abc"))],
            Some("abc")
        )
        .is_ok());
    }

    #[test]
    fn readiness_reports_missing_stale_unready_unexpected_and_build_mismatch() {
        let expected = vec![
            "missing".to_string(),
            "stale".to_string(),
            "dead".to_string(),
            "wrong-build".to_string(),
        ];
        let error = require_worker_readiness(
            &expected,
            &[
                worker("stale", "idle", 16.0, Some("abc")),
                worker("dead", "suspected_dead", 1.0, Some("abc")),
                worker("wrong-build", "idle", 1.0, Some("old")),
                worker("other", "idle", 1.0, Some("abc")),
            ],
            Some("abc"),
        )
        .unwrap_err()
        .to_string();
        for diagnostic in [
            "missing registrations",
            "stale registrations",
            "unready registrations",
            "unexpected registrations",
            "build mismatches",
            "did not mutate",
        ] {
            assert!(error.contains(diagnostic), "{error}");
        }
    }

    #[test]
    fn readiness_rejects_non_ready_status_and_invalid_heartbeat_ages() {
        let expected = vec!["pool-worker-0".to_string()];
        for (status, age) in [
            ("starting", 1.0),
            ("suspected_dead", 1.0),
            ("idle", -1.0),
            ("active", f64::NAN),
            ("active", f64::INFINITY),
        ] {
            assert!(require_worker_readiness(
                &expected,
                &[worker("pool-worker-0", status, age, Some("abc"))],
                Some("abc")
            )
            .is_err());
        }
    }

    #[test]
    fn stale_unmanaged_registration_breaks_exact_vm_set() {
        let expected = vec!["pool-worker-0".to_string()];
        let error = require_worker_readiness(
            &expected,
            &[
                worker("pool-worker-0", "idle", 1.0, Some("abc")),
                worker("deleted-worker", "suspected_dead", 60.0, Some("abc")),
            ],
            Some("abc"),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unexpected registrations"), "{error}");
    }
}
