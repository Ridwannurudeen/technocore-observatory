#!/usr/bin/env python3
"""Fail when the current Observatory publication is stale.

Two independent conditions, either of which fails the check:

  1. the current release is more than 30 minutes old (the rebuild stopped landing);
  2. the release's published `valid_until` is more than 10 minutes in the past
     (releases keep landing, but each one is already past the validity it promises).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


MAX_AGE_SECONDS = 30 * 60
VALIDITY_GRACE_SECONDS = 10 * 60
DEFAULT_CURRENT = Path("/opt/technocore-observatory/current")
RELEASE_NAME = re.compile(r"(?P<timestamp>\d{14})-[0-9a-fA-F]+")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current", nargs="?", type=Path, default=DEFAULT_CURRENT)
    return parser


def release_time(release: Path) -> float:
    match = RELEASE_NAME.fullmatch(release.name)
    if match is not None:
        try:
            timestamp = datetime.strptime(match["timestamp"], "%Y%m%d%H%M%S")
        except ValueError:
            pass
        else:
            return timestamp.replace(tzinfo=timezone.utc).timestamp()
    return release.stat().st_mtime


def validity_deadline(release: Path) -> float:
    status_path = release / "api" / "v1" / "status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    valid_until = status.get("valid_until") if isinstance(status, dict) else None
    if not isinstance(valid_until, str):
        raise ValueError(f"{status_path} carries no valid_until")
    try:
        deadline = datetime.strptime(valid_until, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise ValueError(f"{status_path} valid_until is not UTC: {error}") from None
    return deadline.replace(tzinfo=timezone.utc).timestamp()


def main() -> int:
    current = build_parser().parse_args().current
    try:
        release = current.resolve(strict=True)
        if not release.is_dir():
            raise OSError(f"resolved release is not a directory: {release}")
        published_at = release_time(release)
    except OSError as error:
        print(
            f"publication staleness check failed for {current}: {error}",
            file=sys.stderr,
        )
        return 1

    age = time.time() - published_at
    if age > MAX_AGE_SECONDS:
        print(
            f"publication is stale: age={age:.1f}s release={release}", file=sys.stderr
        )
        return 1

    try:
        deadline = validity_deadline(release)
    except (OSError, ValueError) as error:
        print(
            f"publication validity check failed for {release}: {error}",
            file=sys.stderr,
        )
        return 1
    overdue = time.time() - deadline
    if overdue > VALIDITY_GRACE_SECONDS:
        print(
            f"publication is past its validity: overdue={overdue:.1f}s "
            f"release={release}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
