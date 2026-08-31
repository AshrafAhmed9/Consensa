"""Command-line entry point for running or replaying deterministic harness histories."""

import argparse
from pathlib import Path

from harness.torture.history import History, read_history, write_history
from harness.torture.nemesis import schedule
from harness.torture.workload import register, vector


def execute(seed: int, workload: str, nemeses: list[str]) -> bool:
    """Runs one deterministic reference workload and records only a failing schedule."""
    passed = {"register": register.run, "vector": vector.run}[workload]()
    events = [fault.__dict__ for fault in schedule(seed, nemeses, 20)]
    if not passed:
        write_history(Path("harness/torture/results") / f"seed-{seed}.json", History(seed, workload, events))
    return passed


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--workload", choices=["register", "vector"], required=True)
    run.add_argument("--nemesis", default="partition,crash")
    run.add_argument("--seeds", type=int, default=1)
    replay = sub.add_parser("replay")
    replay.add_argument("--history", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "replay":
        saved = read_history(args.history)
        return 0 if execute(saved.seed, saved.workload, [event["kind"] for event in saved.events]) else 1
    return 0 if all(execute(seed, args.workload, args.nemesis.split(",")) for seed in range(args.seeds)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
