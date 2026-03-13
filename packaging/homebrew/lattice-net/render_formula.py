#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path


def read_checksum(path: Path) -> str:
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        raise ValueError(f"checksum file is empty: {path}")
    return content.split()[0]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render the lattice-net Homebrew formula for a release."
    )
    parser.add_argument("--version", required=True, help="Release version without prefix")
    parser.add_argument("--macos-x86-sha256", required=True, type=Path)
    parser.add_argument("--macos-arm64-sha256", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    template_path = script_dir / "Formula" / "lattice-net.rb.template"
    template = template_path.read_text(encoding="utf-8")

    rendered = (
        template.replace("REPLACE_WITH_VERSION", args.version)
        .replace("REPLACE_WITH_MACOS_X86_64_SHA256", read_checksum(args.macos_x86_sha256))
        .replace(
            "REPLACE_WITH_MACOS_AARCH64_SHA256",
            read_checksum(args.macos_arm64_sha256),
        )
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
