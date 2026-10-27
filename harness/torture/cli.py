"""Command line for the torture harness: run the register workload for many seeds."""

import argparse

from harness.torture.workload import register


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run", choices=["run"])
    parser.add_argument("--nemesis", default="partition,crash", help="comma-separated fault kinds")
    parser.add_argument("--seeds", type=int, default=1, help="run seeds 0 up to this number")
    args = parser.parse_args()
    nemeses = args.nemesis.split(",")
    failed = [seed for seed in range(args.seeds) if not register.run(seed, nemeses)]
    for seed in failed:
        print(f"seed {seed}: history is NOT linearizable (rerun with the same seed to reproduce)")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
