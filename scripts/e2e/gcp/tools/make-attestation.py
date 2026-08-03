#!/usr/bin/env python3
"""Generate and validate canonical custom-worker artifact attestations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import sys
import tempfile
from typing import Any

SCHEMA_VERSION = 1
PLATFORM = "linux"
ARCHITECTURE = "x86_64"
EXPECTED_FIELDS = {
    "arch",
    "build_identity",
    "build_version",
    "platform",
    "revision",
    "schema_version",
    "sha256",
    "size",
}
REVISION_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
BUILD_IDENTITY_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?\Z")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
CHUNK_SIZE = 1024 * 1024
MAX_ATTESTATION_BYTES = 16 * 1024


class AttestationError(ValueError):
    """An artifact or attestation violates the fixture contract."""


def canonical_bytes(document: dict[str, Any]) -> bytes:
    """Serialize one attestation deterministically for build output."""
    return (
        json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("ascii")


# Retain the private spelling for callers that used the initial fixture draft.
_canonical_bytes = canonical_bytes


def _validate_revision(value: str, label: str = "revision") -> None:
    if not isinstance(value, str) or REVISION_RE.fullmatch(value) is None:
        raise AttestationError(f"{label} must be a lowercase, full 40- or 64-character object ID")


def _validate_build_identity(value: str, label: str = "build identity") -> None:
    if not isinstance(value, str) or BUILD_IDENTITY_RE.fullmatch(value) is None:
        raise AttestationError(
            f"{label} must be a lowercase ASCII identifier of at most 32 characters"
        )


def _inspect_elf(binary: Path) -> tuple[int, str]:
    try:
        mode = binary.stat().st_mode
        if not stat.S_ISREG(mode):
            raise AttestationError(f"artifact is not a regular file: {binary}")
        with binary.open("rb") as stream:
            header = stream.read(64)
    except OSError as error:
        raise AttestationError(f"cannot read artifact {binary}: {error}") from error

    if len(header) < 64 or header[:4] != b"\x7fELF":
        raise AttestationError("artifact is not an ELF file with a complete ELF64 header")
    if header[4] != 2:
        raise AttestationError("artifact must be ELF64")
    if header[5] != 1:
        raise AttestationError("artifact must use little-endian ELF encoding")
    if header[6] != 1:
        raise AttestationError("artifact has an unsupported ELF identification version")

    machine = struct.unpack_from("<H", header, 18)[0]
    if machine != 62:
        raise AttestationError(
            f"expected x86_64 artifact (ELF machine 62), found machine {machine}"
        )

    digest = hashlib.sha256()
    size = 0
    try:
        with binary.open("rb") as stream:
            while chunk := stream.read(CHUNK_SIZE):
                digest.update(chunk)
                size += len(chunk)
    except OSError as error:
        raise AttestationError(f"cannot hash artifact {binary}: {error}") from error
    return size, digest.hexdigest()


def _require_embedded_identity(binary: Path, build_identity: str) -> None:
    try:
        content = binary.read_bytes()
    except OSError as error:
        raise AttestationError(f"cannot read artifact {binary}: {error}") from error
    if build_identity.encode("ascii") not in content:
        raise AttestationError("artifact does not embed build_identity")


def make_document(
    binary: Path, revision: str, build_identity: str
) -> dict[str, Any]:
    _validate_revision(revision)
    _validate_build_identity(build_identity)
    size, digest = _inspect_elf(binary)
    full_identity = f"e2e:{revision}:{build_identity}"
    _require_embedded_identity(binary, full_identity)
    return {
        "arch": ARCHITECTURE,
        "build_identity": full_identity,
        "build_version": full_identity,
        "platform": PLATFORM,
        "revision": revision,
        "schema_version": SCHEMA_VERSION,
        "sha256": digest,
        "size": size,
    }


def _reject_constant(value: str) -> None:
    raise AttestationError(f"invalid JSON constant: {value}")


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AttestationError(f"duplicate attestation field: {key}")
        result[key] = value
    return result


def _load_attestation(path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise AttestationError(f"cannot read attestation {path}: {error}") from error
    if len(raw) > MAX_ATTESTATION_BYTES:
        raise AttestationError("attestation exceeds the 16 KiB size limit")
    try:
        document = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AttestationError(f"attestation is not valid UTF-8 JSON: {error}") from error
    if not isinstance(document, dict):
        raise AttestationError("attestation root must be a JSON object")
    return document, raw


def _validate_document(document: dict[str, Any]) -> None:
    fields = set(document)
    if fields != EXPECTED_FIELDS:
        missing = sorted(EXPECTED_FIELDS - fields)
        unknown = sorted(fields - EXPECTED_FIELDS)
        raise AttestationError(
            f"attestation fields mismatch strict schema; missing={missing}, unknown={unknown}"
        )
    if type(document["schema_version"]) is not int or document["schema_version"] != SCHEMA_VERSION:
        raise AttestationError(f"schema_version must be {SCHEMA_VERSION}")
    if document["platform"] != PLATFORM:
        raise AttestationError(f"platform must be {PLATFORM!r}")
    if document["arch"] != ARCHITECTURE:
        raise AttestationError(f"arch must be {ARCHITECTURE!r}")

    revision = document["revision"]
    _validate_revision(revision)
    identity_prefix = f"e2e:{revision}:"
    build_identity = document["build_identity"]
    if not isinstance(build_identity, str) or not build_identity.startswith(identity_prefix):
        raise AttestationError("build_identity must contain the full revision")
    _validate_build_identity(build_identity.removeprefix(identity_prefix))
    if "-dirty" in build_identity or "development-build" in build_identity:
        raise AttestationError("build_identity must describe a clean build")
    build_version = document["build_version"]
    if not isinstance(build_version, str) or re.fullmatch(r"[A-Za-z0-9._/+,:=-]+", build_version) is None:
        raise AttestationError("build_version must be non-empty and deployment-safe")
    size = document["size"]
    if type(size) is not int or size < 64:
        raise AttestationError("size must be an integer of at least 64")
    digest = document["sha256"]
    if not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None:
        raise AttestationError("sha256 must be exactly 64 lowercase hexadecimal characters")


def validate_attestation(
    binary: Path,
    attestation: Path,
    expected_revision: str,
    expected_build_identity: str,
) -> dict[str, Any]:
    _validate_revision(expected_revision, "expected_revision")
    _validate_build_identity(expected_build_identity, "expected_build_identity")

    document, _raw = _load_attestation(attestation)
    _validate_document(document)
    if document["revision"] != expected_revision:
        raise AttestationError("attestation revision does not match expected revision")
    full_identity = f"e2e:{expected_revision}:{expected_build_identity}"
    if document["build_identity"] != full_identity:
        raise AttestationError("attestation build_identity does not match expected build identity")

    size, digest = _inspect_elf(binary)
    if document["size"] != size:
        raise AttestationError("attestation size does not match artifact")
    if document["sha256"] != digest:
        raise AttestationError("attestation sha256 does not match artifact")
    _require_embedded_identity(binary, document["build_identity"])
    return document


def create(
    binary: Path, revision: str, build_identity: str
) -> dict[str, Any]:
    """Library API for constructing an attestation without writing a sidecar."""
    return make_document(Path(binary), revision, build_identity)


def verify(
    binary: Path,
    attestation: Path,
    expected_revision: str | None = None,
    expected_build_identity: str | None = None,
) -> dict[str, Any]:
    """Library API for strict validation, with optional external expectations."""
    document, _ = _load_attestation(Path(attestation))
    _validate_document(document)
    return validate_attestation(
        Path(binary),
        Path(attestation),
        document["revision"] if expected_revision is None else expected_revision,
        document["build_identity"].removeprefix(f"e2e:{document['revision']}:")
        if expected_build_identity is None
        else expected_build_identity,
    )


def _write_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary_name, 0o644)
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)

    create = subparsers.add_parser("create", help="generate a canonical attestation")
    create.add_argument("--binary", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--revision", required=True)
    create.add_argument("--build-identity", required=True)

    validate = subparsers.add_parser(
        "validate", aliases=["verify"], help="strictly validate an artifact and sidecar"
    )
    validate.add_argument("--binary", type=Path, required=True)
    validate.add_argument("--attestation", type=Path, required=True)
    validate.add_argument("--expected-revision", required=True)
    validate.add_argument("--expected-build-identity", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.action == "create":
            if args.binary.resolve() == args.output.resolve():
                raise AttestationError("attestation output must not overwrite the artifact")
            document = make_document(
                args.binary, args.revision, args.build_identity
            )
            _write_atomic(args.output, _canonical_bytes(document))
            validate_attestation(
                args.binary,
                args.output,
                args.revision,
                args.build_identity,
            )
            print(args.output)
        else:
            validate_attestation(
                args.binary,
                args.attestation,
                args.expected_revision,
                args.expected_build_identity,
            )
            print(args.attestation)
    except (AttestationError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
