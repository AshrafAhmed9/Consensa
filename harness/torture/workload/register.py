"""Register workload: runs cmd/torture (a Raft cluster in one process) under a fault
schedule made from a seed, then checks the reads and writes it recorded for linearizability.
The same seed always gives the same fault schedule, so a failing seed can be replayed.
"""

import json
import subprocess
from pathlib import Path

from harness.torture.checker.linearizability import Operation, is_linearizable
from harness.torture.nemesis import schedule

REPO_ROOT = Path(__file__).resolve().parents[3]
_BINARY_PATH: Path | None = None


def _binary() -> Path:
    """Builds cmd/torture the first time and reuses it, so a run does not rebuild per seed."""
    global _BINARY_PATH
    if _BINARY_PATH is None:
        out = REPO_ROOT / "harness" / "torture" / ".torture-bin"
        result = subprocess.run(
            ["go", "build", "-o", str(out), "./cmd/torture"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"building cmd/torture failed:\n{result.stderr}")
        _BINARY_PATH = out
    return _BINARY_PATH


def run(seed: int = 0, nemeses: list[str] | None = None, steps: int = 20, nodes: int = 3) -> bool:
    """Runs one seed. Returns True if the recorded history is linearizable."""
    faults = schedule(seed, nemeses or [], steps, nodes)
    faults_json = json.dumps([f.__dict__ for f in faults])

    result = subprocess.run(
        [str(_binary()), "-nodes", str(nodes), "-rounds", str(steps)],
        input=faults_json, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cmd/torture failed for seed={seed} nemeses={nemeses}:\n{result.stderr}")

    history = [Operation(**op) for op in json.loads(result.stdout)["history"]]
    return is_linearizable(history)
