#!/usr/bin/env python3

import argparse
import json
import os
import shutil
import sys
import tempfile
import urllib.parse
import urllib.request


def fetch_json(url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "lattice-release-workflow",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def find_version(addon_ref: str, version: str, channel: str | None) -> dict | None:
    encoded_ref = urllib.parse.quote(addon_ref, safe="")
    next_url = (
        f"https://addons.mozilla.org/api/v5/addons/addon/{encoded_ref}/versions/"
        "?page_size=100"
    )

    while next_url:
        payload = fetch_json(next_url)
        for result in payload.get("results", []):
            if result.get("version") != version:
                continue
            if channel and result.get("channel") != channel:
                continue
            return result
        next_url = payload.get("next")

    return None


def download_file(url: str, output_path: str) -> None:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "lattice-release-workflow"},
    )
    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)

    fd, temp_path = tempfile.mkstemp(prefix="amo-xpi-", dir=output_dir)
    os.close(fd)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            with open(temp_path, "wb") as handle:
                shutil.copyfileobj(response, handle)
        os.replace(temp_path, output_path)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download a signed XPI for an already-published AMO version."
    )
    parser.add_argument("--addon-ref", required=True, help="AMO slug or Gecko add-on ID")
    parser.add_argument("--version", required=True, help="Extension version to fetch")
    parser.add_argument("--output", required=True, help="Path to write the XPI to")
    parser.add_argument(
        "--channel",
        default=None,
        help="Optional AMO channel to require when matching the version",
    )
    args = parser.parse_args()

    try:
        matched_version = find_version(args.addon_ref, args.version, args.channel)
    except Exception as exc:
        print(f"Failed to query AMO API: {exc}", file=sys.stderr)
        return 1

    if not matched_version:
        channel_note = f" on channel {args.channel}" if args.channel else ""
        print(
            f"AMO does not currently expose version {args.version}{channel_note} "
            f"for add-on {args.addon_ref}.",
            file=sys.stderr,
        )
        return 1

    file_info = matched_version.get("file") or {}
    file_url = file_info.get("url")
    if not file_url:
        print(
            f"AMO version {args.version} does not expose a downloadable signed XPI yet.",
            file=sys.stderr,
        )
        return 1

    try:
        download_file(file_url, args.output)
    except Exception as exc:
        print(f"Failed to download signed XPI from AMO: {exc}", file=sys.stderr)
        return 1

    print(f"Downloaded AMO signed XPI for version {args.version} to {args.output}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
