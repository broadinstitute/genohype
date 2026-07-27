# GCP Pool Stress-Test Walkthrough

**Start with [Installation](../../README.md#installation), then complete the [minimal pool walkthrough](minimal.md) through the dashboard connection.** This walkthrough assumes its `PROJECT`, `ZONE`, `POOL`, and `GENOHYPE_CONFIG` environment variables are still set and that the coordinator and one worker are running.

The synthetic `stress` workload exercises scheduling and telemetry without requiring a genomic dataset. Each partition can consume controlled CPU time and memory, making it useful for learning the pool UI and CLI monitoring commands.

> [!WARNING]
> Stress parameters are applied per task. Begin with the small values below; large memory values or high concurrency can exhaust a worker.

## 1. Confirm the pool is healthy

```bash
genohype --config "$GENOHYPE_CONFIG" pool list "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool status "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool workers "$POOL"
```

If the IAP tunnel from the minimal walkthrough is not running, start it and leave it open:

```bash
gcloud compute ssh "${POOL}-coordinator" \
  --project "$PROJECT" \
  --zone "$ZONE" \
  --tunnel-through-iap \
  -- -N -L 3000:localhost:3000
```

Open [http://localhost:3000/dashboard](http://localhost:3000/dashboard). Before submission, the coordinator should be idle and `${POOL}-worker-0` should be connected.

## 2. Start observers

Use separate terminals so you can compare the CLI, dashboard, and GCP views while the submission command streams logs.

### Terminal A: pool events

```bash
genohype --config "$GENOHYPE_CONFIG" pool events "$POOL" --follow
```

### Terminal B: GCP VM state

```bash
while true; do
  clear
  date
  gcloud compute instances list \
    --project "$PROJECT" \
    --filter="name~'^${POOL}-'" \
    --format='table(name,status,cpuPlatform,scheduling.provisioningModel,lastStartTimestamp)'
  sleep 3
done
```

A stress submission does not create additional VMs unless you explicitly scale or use a configured autoscale workflow. This observer should therefore remain at one coordinator and one worker throughout the job.

### Terminal C: API telemetry

With the IAP tunnel running:

```bash
while true; do
  clear
  date
  curl -fsS http://localhost:3000/api/dashboard/summary
  echo
  curl -fsS http://localhost:3000/api/dashboard/workers
  sleep 2
done
```

If `jq` is installed, append `| jq .` to either `curl` command for formatted JSON.

## 3. Submit a small CPU and memory stress test

In **terminal D**:

```bash
genohype --config "$GENOHYPE_CONFIG" pool submit "$POOL" \
  --batch-size 2 \
  -- \
  stress \
  --partitions 24 \
  --cpu-secs 2 \
  --memory-mb 256
```

This queues 24 synthetic partitions. Each partition performs approximately two seconds of CPU work and allocates 256 MiB during its task. `--batch-size 2` asks the coordinator to assign two partitions per worker request.

While it runs, observe:

- **Dashboard summary:** queued, running, and completed tasks
- **Worker view:** heartbeat freshness, CPU, memory, and current tasks
- **Events:** worker requests, assignments, completions, and final job state
- **GCP:** VM count remains unchanged because scheduling occurs inside the existing pool

The submit command follows coordinator output and returns after the coordinator reports completion. A successful run should return the coordinator to idle mode.

## 4. Inspect the completed run

Stop the observer loops with **Ctrl-C**, then inspect the final state:

```bash
genohype --config "$GENOHYPE_CONFIG" pool status "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool workers "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool events "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool failures "$POOL"

curl -fsS http://localhost:3000/api/dashboard/summary
curl -fsS http://localhost:3000/api/dashboard/bottlenecks
```

The dashboard intentionally retains the most recent job summary after completion, so you can inspect throughput and utilization while the coordinator is idle.

## 5. Observe scaling resources up and down

The minimal profile targets one worker, but manual scaling is useful for seeing the VM lifecycle independently of a job.

In one terminal, restart the GCP observer from step 2. In another, scale from one worker to two:

```bash
genohype --config "$GENOHYPE_CONFIG" pool scale "$POOL" --workers 2
```

Watch `${POOL}-worker-1` move through provisioning and startup. Confirm both workers register:

```bash
genohype --config "$GENOHYPE_CONFIG" pool list "$POOL"
genohype --config "$GENOHYPE_CONFIG" pool workers "$POOL"
```

Run a slightly larger stress job to see tasks shared dynamically:

```bash
genohype --config "$GENOHYPE_CONFIG" pool submit "$POOL" \
  --batch-size 2 \
  -- \
  stress --partitions 40 --cpu-secs 2 --memory-mb 256
```

Then scale back to one worker and watch the highest-index worker disappear:

```bash
genohype --config "$GENOHYPE_CONFIG" pool scale "$POOL" --workers 1

gcloud compute instances list \
  --project "$PROJECT" \
  --filter="name~'^${POOL}-'" \
  --format='table(name,status,zone.basename())'
```

Do not scale workers down during this introductory stress run. Worker loss is recoverable, but intentionally testing rescheduling deserves a separate failure-injection exercise.

## 6. Optional workload variations

### CPU-focused

```bash
genohype --config "$GENOHYPE_CONFIG" pool submit "$POOL" -- \
  stress --partitions 40 --cpu-secs 5 --memory-mb 64
```

### Memory-focused

```bash
genohype --config "$GENOHYPE_CONFIG" pool submit "$POOL" \
  --memory-weight-mb 512 \
  -- \
  stress --partitions 12 --cpu-secs 0.5 --memory-mb 512
```

`--memory-weight-mb` gives the scheduler a per-partition memory hint. Keep the requested memory comfortably below the worker’s available RAM after accounting for concurrent tasks and operating-system overhead.

### Automatically stop VMs after completion

```bash
genohype --config "$GENOHYPE_CONFIG" pool submit "$POOL" \
  --auto-stop \
  -- \
  stress --partitions 24 --cpu-secs 2 --memory-mb 256
```

`--auto-stop` stops, rather than deletes, the coordinator and workers. Stopped VMs no longer incur CPU charges, but their disks remain and the pool cannot accept another job until the VMs are started. For this walkthrough, prefer explicit `pool destroy` cleanup unless you specifically want to inspect stopped resources.

## 7. Clean up

If you did not use `--auto-stop`, return to the [destroy and verify](minimal.md#5-destroy-and-verify-the-pool) section of the minimal walkthrough.

If you used `--auto-stop`, the same destroy command will find and delete the stopped instances:

```bash
genohype --config "$GENOHYPE_CONFIG" pool destroy "$POOL"
```

Then remove the best-effort per-pool firewall rule and temporary configuration as described in the minimal walkthrough. Verify instances, disks, and pool-named firewall rules are all absent before ending the exercise.

## Troubleshooting

### No workers appear in the dashboard

```bash
gcloud compute ssh "${POOL}-worker-0" \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
  -- sudo systemctl status genohype-worker --no-pager

gcloud compute ssh "${POOL}-worker-0" \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
  -- sudo journalctl -u genohype-worker -n 100 --no-pager
```

### Coordinator or UI is unavailable

```bash
gcloud compute ssh "${POOL}-coordinator" \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
  -- sudo systemctl status genohype-coordinator --no-pager

gcloud compute ssh "${POOL}-coordinator" \
  --project "$PROJECT" --zone "$ZONE" --tunnel-through-iap \
  -- sudo journalctl -u genohype-coordinator -n 100 --no-pager
```

### Submission says another job is running

Check `pool status` and the dashboard first. Use `pool cancel "$POOL"` when the existing job should be cancelled. Reserve `pool submit --force` for intentionally superseding a verified job.
