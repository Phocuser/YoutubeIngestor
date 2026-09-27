"""One-shot entry point for the managed YouTube source worker.

It performs one lease/process/complete pass and exits. No work starts on
module import; the URL is supplied explicitly and the source token is read
from the protected ``MYCELIUM_YOUTUBE_SOURCE_TOKEN`` environment variable.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
import sys

from .managed_worker import ControlPlaneError, LeaseLost, ManagedYouTubeWorker, MyceliumSourceControlClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process one bounded managed YouTube job batch")
    parser.add_argument("--mycelium-url", required=True, help="Mycelium API base URL")
    parser.add_argument("--worker-id", required=True, help="Stable worker identifier")
    parser.add_argument("--lease-seconds", type=int, default=120)
    parser.add_argument("--batch-limit", type=int, default=1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.batch_limit != 1:
        print("--batch-limit must be 1; process one leased job per pass", file=sys.stderr)
        return 2
    token = os.environ.get("MYCELIUM_YOUTUBE_SOURCE_TOKEN", "").strip()
    if not token:
        print("MYCELIUM_YOUTUBE_SOURCE_TOKEN is required", file=sys.stderr)
        return 2
    control = MyceliumSourceControlClient(args.mycelium_url, token)
    try:
        outcomes = ManagedYouTubeWorker(
            control,
            worker_id=args.worker_id,
            lease_seconds=args.lease_seconds,
            batch_limit=args.batch_limit,
        ).run_once()
    except (ControlPlaneError, LeaseLost, ValueError) as exc:
        print(json.dumps({"status": "worker_error", "error": str(exc)}), file=sys.stderr)
        return 2
    finally:
        control.close()
    print(json.dumps([asdict(outcome) for outcome in outcomes], sort_keys=True))
    return 0 if all(outcome.completed and outcome.status == "succeeded" for outcome in outcomes) else 2


if __name__ == "__main__":
    raise SystemExit(main())
