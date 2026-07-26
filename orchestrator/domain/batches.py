from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    request_payload: dict[str, Any]
    output_dir: Path


@dataclass(frozen=True)
class BatchPlan:
    batch_id: str
    jobs: list[JobRecord]
