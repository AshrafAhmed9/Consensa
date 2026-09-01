"""Register workload: drives a real internal/raft.Cluster (via cmd/torture) under a seeded
fault schedule and checks the resulting real operation history for linearizability.

Before this existed, run() checked a fixed, hand-written two-operation history that never
touched Go and never depended on the seed or nemesis schedule at all -- torture run
--seeds 5000 would have repeated the identical check 5000 times and always passed,
regardless of whether Consensa's Raft implementation actually had a bug. See
docs/notes/06-torture.md for the full story.
"""

import json
import subprocess
from pathlib import Path

from harness.torture.checker.linearizability import Operation, is_linearizable
from harness.torture.nemesis import schedule

REPO_ROOT = Path(__file__).resolve().parents[3]
_BINARY_PATH: Path | None = None


def _binary() -> Path:
    """Builds cmd/torture once per process and reuses it -- rebuilding per seed would waste
    most of a run's wall-clock time on `go build` rather than actual fault injection."""
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
    """Drives a real Raft cluster under a seeded fault schedule and checks the resulting
    real operation history for linearizability. The same seed and nemeses always produce
    the same fault schedule (harness/torture/nemesis.py's schedule is a pure function of
    them), so a failing seed is reproducible.
    """
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
