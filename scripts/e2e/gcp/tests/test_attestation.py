import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

TOOL_PATH = Path(__file__).parents[1] / "tools" / "make-attestation.py"
SPEC = importlib.util.spec_from_file_location("make_attestation", TOOL_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

REVISION = "0123456789abcdef0123456789abcdef01234567"


def fake_elf(path: Path, machine: int = 62) -> None:
    header = bytearray(64)
    header[:4] = b"\x7fELF"
    header[4] = 2
    header[5] = 1
    header[6] = 1
    header[18:20] = machine.to_bytes(2, "little")
    path.write_bytes(header + f"e2e:{REVISION}:fixture-a".encode())


class AttestationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.binary = self.root / "worker"
        self.sidecar = self.root / "worker.attestation.json"
        fake_elf(self.binary)
        value = MODULE.create(self.binary, REVISION, "fixture-a")
        self.sidecar.write_bytes(MODULE.canonical_bytes(value))

    def tearDown(self):
        self.temp.cleanup()

    def test_round_trip_is_canonical_and_bound_to_bytes(self):
        value = MODULE.verify(
            self.binary,
            self.sidecar,
            expected_revision=REVISION,
            expected_build_identity="fixture-a",
        )
        self.assertEqual(value["build_version"], f"e2e:{REVISION}:fixture-a")
        self.assertEqual(
            self.sidecar.read_bytes(),
            (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        )

    def test_altered_bytes_are_rejected(self):
        self.binary.write_bytes(self.binary.read_bytes() + b"tampered")
        with self.assertRaisesRegex(MODULE.AttestationError, "does not match artifact"):
            MODULE.verify(self.binary, self.sidecar)

    def test_unknown_fields_are_rejected_but_wire_format_matches_product(self):
        value = json.loads(self.sidecar.read_text())
        value["claim"] = "unsigned"
        self.sidecar.write_text(json.dumps(value))
        with self.assertRaisesRegex(MODULE.AttestationError, "fields mismatch"):
            MODULE.verify(self.binary, self.sidecar)

        # Rust serde accepts ordinary JSON formatting; canonical output is a
        # deterministic generator property, not an extra validator schema.
        value.pop("claim")
        self.sidecar.write_text(json.dumps(value, indent=2))
        self.assertEqual(MODULE.verify(self.binary, self.sidecar), value)

    def test_revision_build_and_elf_machine_are_strict(self):
        with self.assertRaisesRegex(MODULE.AttestationError, "revision"):
            MODULE.create(self.binary, REVISION.upper(), "fixture-a")
        with self.assertRaisesRegex(MODULE.AttestationError, "build identity"):
            MODULE.create(self.binary, REVISION, "fixture_a")
        fake_elf(self.binary, machine=183)
        with self.assertRaisesRegex(MODULE.AttestationError, "expected x86_64"):
            MODULE.create(self.binary, REVISION, "fixture-a")


if __name__ == "__main__":
    unittest.main()
