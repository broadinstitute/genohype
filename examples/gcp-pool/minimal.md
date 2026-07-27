# Minimal GCP Pool Walkthrough

**Start with [Installation](../../README.md#installation).** A release installation includes the Linux worker binary used by pool commands. If you build Genohype from source, build that binary with `make worker` before continuing.

This walkthrough creates the smallest useful distributed pool—one coordinator and one worker—shows how to watch its GCP resources appear, opens the pool dashboard through IAP, and then removes the resources.

> [!WARNING]
> This walkthrough creates billable GCP resources. Complete the cleanup section even if an earlier command fails.

## 1. Prerequisites

You need:

- `genohype`, `genohype-worker`, `gcloud`, and `curl`
- a GCP project with billing enabled
- permission to use Compute Engine and create/delete instances and firewall rules
- Application Default Credentials if jobs will read or write GCS
- To use the optional IAP dashboard tunnel, permission to use IAP and a VPC rule allowing IAP's range (`35.235.240.0/20`) to reach the coordinator on TCP port 22. Many managed GCP projects already provide this; it is not required when you use another approved access path or do not open the UI.

Authenticate and enable Compute Engine:

```bash
gcloud auth login
gcloud auth application-default login

export PROJECT="$(gcloud config get-value project)"
export ZONE="us-central1-a"

gcloud services enable compute.googleapis.com --project "$PROJECT"
```

Verify the local tools and select a unique pool name:

```bash
genohype --version
command -v genohype-worker

export POOL="genohype-minimal-$(date +%s)"
echo "Project: $PROJECT"
echo "Zone:    $ZONE"
echo "Pool:    $POOL"
```

## 2. Create a temporary pool configuration

Using a configuration file ensures every pool command uses the same project and zone instead of depending on changing ambient `gcloud` settings.

```bash
export GENOHYPE_CONFIG="$(mktemp)"
cat > "$GENOHYPE_CONFIG" <<EOF
[defaults]
project = "$PROJECT"
zone = "$ZONE"

[pools.$POOL]
machine_type = "n1-standard-4"
starting_workers = 1
workers = 1
spot = false
with_coordinator = true
EOF

cat "$GENOHYPE_CONFIG"
```

In the commands below, `--config` is a global option and therefore appears before `pool`.

## 3. Watch GCP while the pool is created

In **terminal 1**, start an observer before creating the pool:

```bash
while true; do
  clear
  date
  gcloud compute instances list \
    --project "$PROJECT" \
    --filter="name~'^${POOL}-'" \
    --format='table(name,zone.basename(),status,machineType.basename(),scheduling.provisioningModel,networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)'
  echo
  gcloud compute disks list \
    --project "$PROJECT" \
    --filter="name~'^${POOL}-'" \
    --format='table(name,zone.basename(),status,sizeGb,type.basename())'
  sleep 2
done
```

In **terminal 2**, create the pool and wait for VM startup to finish:

```bash
genohype --config "$GENOHYPE_CONFIG" pool create "$POOL" --wait
```

You should see these instances appear:

- `${POOL}-coordinator`: an on-demand `e2-standard-2` VM running the coordinator and dashboard
- `${POOL}-worker-0`: the `n1-standard-4` worker requested by the profile

Each VM also gets a boot disk. The default configuration assigns ephemeral external IP addresses. To use a preconfigured private network instead, see [Private-only variation](#private-only-variation).

Stop the observer with **Ctrl-C**, then inspect the completed resources:

```bash
genohype --config "$GENOHYPE_CONFIG" pool list "$POOL"

gcloud compute instances list \
  --project "$PROJECT" \
  --filter="tags.items:pool-${POOL}" \
  --format='table(name,status,zone.basename(),networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)'

gcloud compute firewall-rules describe "allow-hail-coord-int-${POOL}" \
  --project "$PROJECT" \
  --format='yaml(name,network,direction,sourceRanges,allowed)'
```

When Genohype manages networking, it makes a best-effort attempt to create `allow-hail-coord-int-${POOL}`, allowing internal `10.0.0.0/8` traffic to coordinator port 3000. This rule does **not** configure operator access. If you choose the IAP tunnel below, use your project's existing IAP SSH policy or ask an administrator to enable it.

To inspect startup while diagnosing a slow or failed creation:

```bash
gcloud compute instances get-serial-port-output "${POOL}-coordinator" \
  --project "$PROJECT" --zone "$ZONE" --port 1 | tail -100

gcloud compute instances get-serial-port-output "${POOL}-worker-0" \
  --project "$PROJECT" --zone "$ZONE" --port 1 | tail -100
```

## 4. Connect to the pool UI

The dashboard listens on port 3000 of the coordinator. Do not expose it directly; forward it through IAP:

```bash
gcloud compute ssh "${POOL}-coordinator" \
  --project "$PROJECT" \
  --zone "$ZONE" \
  --tunnel-through-iap \
  -- -N -L 3000:localhost:3000
```

Leave that command running. In another terminal:

```bash
curl -fsS http://localhost:3000/status
curl -fsS http://localhost:3000/api/dashboard/summary
curl -fsS http://localhost:3000/api/dashboard/workers
```

Open [http://localhost:3000/dashboard](http://localhost:3000/dashboard) in a browser. The idle pool should show one connected worker. If local port 3000 is already occupied, use `-L 3001:localhost:3000` and open `http://localhost:3001/dashboard` instead.

Proceed to the [stress-test walkthrough](stress-test.md) while the pool and tunnel are running.

## 5. Destroy and verify the pool

Stop the IAP tunnel with **Ctrl-C**. In terminal 1, run the same instance observer from step 3. In terminal 2, destroy the pool:

```bash
genohype --config "$GENOHYPE_CONFIG" pool destroy "$POOL"
```

`pool destroy` lists the VMs it found and deletes them. Watch their states transition to deletion, then verify that the instances and attached boot disks are gone:

```bash
gcloud compute instances list \
  --project "$PROJECT" \
  --filter="name~'^${POOL}-'"

gcloud compute disks list \
  --project "$PROJECT" \
  --filter="name~'^${POOL}-'"
```

At present, `pool destroy` deletes pool VMs but does not delete the per-pool firewall rule. If this walkthrough created it, remove it explicitly:

```bash
if gcloud compute firewall-rules describe "allow-hail-coord-int-${POOL}" \
  --project "$PROJECT" >/dev/null 2>&1; then
  gcloud compute firewall-rules delete "allow-hail-coord-int-${POOL}" \
    --project "$PROJECT" --quiet
fi

rm -f "$GENOHYPE_CONFIG"
```

Perform one final check for pool-named resources:

```bash
gcloud compute instances list --project "$PROJECT" --filter="name~'^${POOL}-'"
gcloud compute disks list --project "$PROJECT" --filter="name~'^${POOL}-'"
gcloud compute firewall-rules list --project "$PROJECT" --filter="name~'${POOL}'"
```

All three commands should return no matching resources.

## Private-only variation

For an existing VPC and subnet that already provide Private Google Access/NAT as needed, set:

```toml
[defaults]
network = "YOUR_VPC"
subnet = "YOUR_SUBNET"
public_ip = false
manage_firewall = false
```

With `public_ip = false`, the VMs are created with `--no-address`. With `manage_firewall = false`, Genohype does not create the port-3000 firewall rule, so your infrastructure must permit worker-to-coordinator traffic. If you choose IAP for operator access, its IAM and SSH firewall policy must also be configured. The IAP dashboard tunnel works without VM external IPs.
