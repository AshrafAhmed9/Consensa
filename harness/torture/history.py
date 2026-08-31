"""Persist failing schedules as JSON so a seed is a reproducible bug report."""

from dataclasses import asdict, dataclass
import json
from pathlib import Path


@dataclass(frozen=True)
class History:
    seed: int
    workload: str
    events: list[dict]


def write_history(path: Path, history: History) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(history), sort_keys=True, indent=2) + "\n")


def read_history(path: Path) -> History:
    return History(**json.loads(path.read_text()))
