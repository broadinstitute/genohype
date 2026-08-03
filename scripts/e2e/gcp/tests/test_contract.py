import re
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[1]


class HarnessContractTests(unittest.TestCase):
    def test_terraform_has_split_keyless_least_privilege_identities(self):
        main = (ROOT / "terraform" / "main.tf").read_text()
        self.assertEqual(main.count('resource "google_service_account"'), 3)
        self.assertNotIn("google_service_account_key", main)
        self.assertNotIn("roles/compute.admin", main)
        self.assertNotRegex(main, r'google_.*_iam_(?:binding|policy)"')
        driver = main[main.index('resource "google_compute_instance" "driver"') :]
        self.assertNotIn("access_config", driver)
        self.assertIn("roles/compute.viewer", main)
        self.assertNotIn("roles/compute.instanceAdmin.v1\"\n  member  = \"serviceAccount:${google_service_account.coordinator_runtime", main)

    def test_generated_stock_and_attested_profiles_are_disjoint(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stock = root / "stock.toml"
            attested = root / "attested.toml"
            library = ROOT / "lib" / "pool-config.sh"
            command = f'''
                set -Eeuo pipefail
                source "{library}"
                write_pool_config "{stock}" stock proj-123 us-east1-b net subnet \\
                  gh-e2e-run123 worker@proj-123.iam.gserviceaccount.com \\
                  coord@proj-123.iam.gserviceaccount.com bucket /release
                write_pool_config "{attested}" attested-custom proj-123 us-east1-b net subnet \\
                  gh-e2e-run123 worker@proj-123.iam.gserviceaccount.com \\
                  coord@proj-123.iam.gserviceaccount.com bucket /release
            '''
            subprocess.run(["bash", "-c", command], check=True)
            stock_pool = tomllib.loads(stock.read_text())["pools"]["gh-e2e-run123"]
            custom_pool = tomllib.loads(attested.read_text())["pools"]["gh-e2e-run123"]

        self.assertNotIn("worker_binary", stock_pool)
        self.assertNotIn("worker_attestation", stock_pool)
        self.assertEqual(custom_pool["worker_binary"], "/release/custom-worker/fixture-a")
        self.assertEqual(
            custom_pool["worker_attestation"],
            "/release/custom-worker/fixture-a.attestation.json",
        )
        guest = (ROOT / "guest-test.sh").read_text()
        self.assertRegex(guest, r"core-smoke\.sh[\s\\\n\"]+.*\$STOCK_CONFIG")
        self.assertRegex(guest, r"pool-lifecycle\.sh[\s\S]+\$ATTESTED_CONFIG")

    def test_attestation_tool_fields_match_product_rust_schema(self):
        tool = (ROOT / "tools" / "make-attestation.py").read_text()
        field_block = tool.split("EXPECTED_FIELDS = {", 1)[1].split("}", 1)[0]
        python_fields = set(re.findall(r'"([a-z_]+)"', field_block))
        rust_path = ROOT.parents[2] / "cli" / "src" / "cloud" / "attestation.rs"
        rust = rust_path.read_text()
        struct_body = rust.split("pub struct WorkerArtifactAttestation {", 1)[1].split("}", 1)[0]
        rust_fields = set(re.findall(r"pub ([a-z_]+):", struct_body))
        self.assertEqual(python_fields, rust_fields)
        self.assertNotIn("protocol_version", python_fields)

    def test_fixture_identity_build_expectations_match_source_keys(self):
        fixture = (ROOT / "fixtures" / "custom-worker" / "src" / "main.rs").read_text()
        identity_body = fixture.split("fn identity() -> Value {", 1)[1].split("\n}", 1)[0]
        source_keys = set(re.findall(r'"([a-z_]+)"\s*:', identity_body))
        build = (ROOT / "build-artifacts.sh").read_text()
        expected_body = build.split("expected = {", 1)[1].split("\n}", 1)[0]
        build_keys = set(re.findall(r'^\s*"([a-z_]+)"\s*:', expected_body, re.MULTILINE))
        expected = {
            "architecture", "build_id", "build_identity", "build_version",
            "platform", "protocol_version", "revision", "schema_version",
            "task_schema_version",
        }
        self.assertEqual(source_keys, expected)
        self.assertEqual(build_keys, expected)
        identity_test = fixture.split("fn identity_is_stable_and_full_revision_bound()", 1)[1]
        self.assertIn("identity(),", identity_test)
        self.assertIn("json!({", identity_test)

    def test_lifecycle_is_one_worker_and_capability_gated(self):
        lifecycle = (ROOT / "scenarios" / "pool-lifecycle.sh").read_text()
        self.assertIn("capability_preflight", lifecycle)
        self.assertIn('--workers 0 --with-coordinator', lifecycle)
        self.assertGreaterEqual(lifecycle.count('--workers 1'), 2)
        self.assertIn('--worker-attestation', lifecycle)
        self.assertIn('--detach --output json', lifecycle)
        self.assertIn('--job-id "$JOB_ID"', lifecycle)
        self.assertNotRegex(lifecycle, re.compile(r"--workers (?:[2-9]|[1-9][0-9]+)"))
        self.assertNotIn("coordinator-scale", lifecycle)
        self.assertNotIn("awk 'NF", lifecycle)
        self.assertIn('submit-stderr.log', lifecycle)

    def test_default_is_one_worker_lifecycle_with_strict_apply_confirmation(self):
        run = (ROOT / "run.sh").read_text()
        self.assertIn('SCENARIOS="${SCENARIOS:-pool-lifecycle}"', run)
        self.assertIn('ROUTINE_WORKERS="${ROUTINE_WORKERS:-1}"', run)
        self.assertIn('MAX_WORKERS="${MAX_WORKERS:-1}"', run)
        self.assertIn('CONFIRM_PROJECT" != "$PROJECT', run)
        self.assertIn('CONFIRM_RUN_ID" != "$RUN_ID', run)
        keep_check = run.index('if [[ "$rc" != 0 && "$KEEP_ON_FAILURE" == 1 ]]')
        cleanup_call = run.index('cleanup.sh" "$STATE_DIR"', keep_check)
        self.assertLess(keep_check, cleanup_call)
        self.assertIn("MIN_GUEST_TIMEOUT_SECS", run)
        self.assertIn("GUEST_CLEANUP_BUDGET_SECS", run)
        self.assertIn("roles/compute.instanceAdmin.v1", run)

    def test_cleanup_has_compound_guard_and_residual_checks(self):
        cleanup = (ROOT / "cleanup.sh").read_text()
        guard = (ROOT / "lib" / "nested-pool.sh").read_text()
        self.assertIn('tags.items:pool-${pool}', guard)
        self.assertIn('"${pool}-worker-0"', guard)
        self.assertIn('"$instance_zone" == "$zone"', guard)
        self.assertIn("verify_nested_pool_empty_stable", cleanup)
        self.assertNotIn("|| true", guard)
        for resource in (
            "instances",
            "disks",
            "addresses",
            "firewall",
            "networks",
            "buckets",
            "service-accounts keys",
            "state list",
            "get-iam-policy",
        ):
            self.assertIn(resource, cleanup)


if __name__ == "__main__":
    unittest.main()
