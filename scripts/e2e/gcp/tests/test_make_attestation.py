#!/usr/bin/env python3
"""Tests for strict custom-worker artifact attestation tooling."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import struct
import tempfile
import unittest

TOOL_PATH = Path(__file__).resolve().parents[1] / "tools" / "make-attestation.py"
SPEC = importlib.util.spec_from_file_location("make_attestation", TOOL_PATH)
assert SPEC is not None and SPEC.loader is not None
make_attestation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(make_attestation)

REVISION = "0123456789abcdef0123456789abcdef01234567"
OTHER_REVISION = "89abcdef0123456789abcdef0123456789abcdef"
BUILD_IDENTITY = "fixture-a"


def elf64_x86_64(payload: bytes | None = None) -> bytes:
    if payload is None:
        payload = f"e2e:{REVISION}:{BUILD_IDENTITY}".encode()
    header = bytearray(64)
    header[:4] = b"\x7fELF"
    header[4] = 2  # ELFCLASS64
    header[5] = 1  # little endian
    header[6] = 1  # ELF identification version
    struct.pack_into("<HHI", header, 16, 3, 62, 1)  # PIE, x86_64, ELF version
    struct.pack_into("<H", header, 52, 64)  # ELF64 header size
    return bytes(header) + payload


class AttestationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.binary = root / "worker"
        self.sidecar = root / "worker.attestation.json"
        self.binary.write_bytes(elf64_x86_64())
        self.binary.chmod(0o755)
        self.document = make_attestation.make_document(
            self.binary, REVISION, BUILD_IDENTITY
        )
        self.write_document(self.document)

    def write_document(self, document: dict[str, object], canonical: bool = True) -> None:
        if canonical:
            payload = make_attestation._canonical_bytes(document)
        else:
            payload = json.dumps(document, indent=2, sort_keys=False).encode() + b"\n"
        self.sidecar.write_bytes(payload)

    def validate(
        self,
        revision: str = REVISION,
        build_identity: str = BUILD_IDENTITY,
    ) -> dict[str, object]:
        return make_attestation.validate_attestation(
            self.binary,
            self.sidecar,
            revision,
            build_identity,
        )

    def test_generation_is_canonical_and_validates(self) -> None:
        self.assertEqual(self.validate(), self.document)
        self.assertEqual(
            self.sidecar.read_bytes(),
            make_attestation._canonical_bytes(self.document),
        )
        self.assertEqual(self.document["build_identity"], f"e2e:{REVISION}:fixture-a")
        self.assertEqual(self.document["build_version"], f"e2e:{REVISION}:fixture-a")
        self.assertEqual(self.document["arch"], "x86_64")
        self.assertEqual(self.document["platform"], "linux")

    def test_altered_artifact_bytes_are_rejected(self) -> None:
        content = self.binary.read_bytes()
        self.binary.write_bytes(content[:-1] + bytes([content[-1] ^ 0xFF]))
        with self.assertRaisesRegex(make_attestation.AttestationError, "sha256"):
            self.validate()

    def test_sidecar_digest_and_size_mismatches_are_rejected(self) -> None:
        cases = {
            "sha256": "0" * 64,
            "size": self.document["size"] + 1,
        }
        for field, value in cases.items():
            with self.subTest(field=field):
                changed = dict(self.document)
                changed[field] = value
                self.write_document(changed)
                with self.assertRaisesRegex(make_attestation.AttestationError, field):
                    self.validate()

    def test_rust_compatible_revision_and_build_version_rules(self) -> None:
        changed = dict(self.document)
        changed["build_version"] = "fixture-release-v1"
        self.write_document(changed)
        self.assertEqual(self.validate(), changed)

        revision_64 = "a" * 64
        self.binary.write_bytes(elf64_x86_64(f"e2e:{revision_64}:{BUILD_IDENTITY}".encode()))
        document = make_attestation.make_document(self.binary, revision_64, BUILD_IDENTITY)
        self.assertEqual(document["revision"], revision_64)

    def test_sidecar_revision_and_build_mismatches_are_rejected(self) -> None:
        changed = dict(self.document)
        changed["revision"] = OTHER_REVISION
        changed["build_identity"] = f"e2e:{OTHER_REVISION}:{BUILD_IDENTITY}"
        changed["build_version"] = f"e2e:{OTHER_REVISION}:{BUILD_IDENTITY}"
        self.write_document(changed)
        with self.assertRaisesRegex(make_attestation.AttestationError, "revision"):
            self.validate()

        changed = dict(self.document)
        changed["build_identity"] = f"e2e:{REVISION}:fixture-b"
        changed["build_version"] = f"e2e:{REVISION}:fixture-b"
        self.write_document(changed)
        with self.assertRaisesRegex(make_attestation.AttestationError, "build_identity"):
            self.validate()

    def test_non_elf_and_unsupported_architecture_are_rejected(self) -> None:
        self.binary.write_bytes(b"not an executable")
        with self.assertRaisesRegex(make_attestation.AttestationError, "ELF"):
            make_attestation.make_document(
                self.binary, REVISION, BUILD_IDENTITY
            )

        unsupported = bytearray(elf64_x86_64())
        struct.pack_into("<H", unsupported, 18, 183)  # AArch64
        self.binary.write_bytes(unsupported)
        with self.assertRaisesRegex(make_attestation.AttestationError, "x86_64"):
            make_attestation.make_document(
                self.binary, REVISION, BUILD_IDENTITY
            )

    def test_unknown_fields_and_noncanonical_digest_casing_are_rejected(self) -> None:
        changed = dict(self.document)
        changed["unexpected"] = True
        self.write_document(changed)
        with self.assertRaisesRegex(make_attestation.AttestationError, "unknown"):
            self.validate()

        changed = dict(self.document)
        changed["sha256"] = str(changed["sha256"]).upper()
        self.write_document(changed)
        with self.assertRaisesRegex(make_attestation.AttestationError, "lowercase"):
            self.validate()

    def test_noncanonical_json_matches_rust_acceptance_but_duplicates_fail(self) -> None:
        self.write_document(self.document, canonical=False)
        self.assertEqual(self.validate(), self.document)

        self.sidecar.write_text(
            '{"schema_version":1,"schema_version":1}\n', encoding="utf-8"
        )
        with self.assertRaisesRegex(make_attestation.AttestationError, "duplicate"):
            self.validate()


if __name__ == "__main__":
    unittest.main()
