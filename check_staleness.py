#!/usr/bin/env python3
"""Fail when the current Observatory publication is more than 30 minutes old."""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


MAX_AGE_SECONDS = 30 * 60
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
