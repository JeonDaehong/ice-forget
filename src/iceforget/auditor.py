"""Auditor: turn an erasure run into a durable, tamper-evident certificate.

The certificate is the artifact a DPO or auditor actually wants: who was
erased, from which table, when it was received and completed, how many rows and
files were involved, which snapshots were expired, and a hash over the whole
body so any later edit is detectable. JSON today; PDF rendering is a thin
downstream concern left to the roadmap so the audit *record* has no heavy deps.
"""

from __future__ import annotations

from pathlib import Path

from iceforget.models import ErasureCertificate, ErasureResult


class Auditor:
    def __init__(self, *, tool_version: str):
        self._tool_version = tool_version

    def certify(self, result: ErasureResult, *, mode: str) -> ErasureCertificate:
        return ErasureCertificate.from_result(
            result, tool_version=self._tool_version, mode=mode
        )

    def write(self, certificate: ErasureCertificate, directory: str | Path) -> Path:
        out_dir = Path(directory)
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{certificate.request_id}.json"
        path.write_text(certificate.to_json(), encoding="utf-8")
        return path
