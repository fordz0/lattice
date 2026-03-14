#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import zipfile


REPO_ROOT = Path(__file__).resolve().parent.parent
EXT_DIR = REPO_ROOT / "lattice-ext"
SKIP_NAMES = {
    "INSTALL.md",
}
SKIP_DIRS = {
    "tests",
    "__pycache__",
}
SKIP_SUFFIXES = {
    ".pyc",
}


def build_manifest(target: str) -> dict:
    manifest = json.loads((EXT_DIR / "manifest.json").read_text(encoding="utf-8"))
    if target == "chrome":
        manifest.pop("browser_specific_settings", None)
    return manifest


def should_skip(path: Path) -> bool:
    if any(part in SKIP_DIRS for part in path.parts):
        return True
    if path.name in SKIP_NAMES:
        return True
    if path.suffix in SKIP_SUFFIXES:
        return True
    return False


def archive_name(target: str, version: str) -> str:
    return f"lattice-extension-{target}-{version}.zip"


def write_zip(target: str, version: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / archive_name(target, version)
    manifest = build_manifest(target)

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for source_path in sorted(EXT_DIR.rglob("*")):
            if source_path.is_dir():
                continue
            rel_path = source_path.relative_to(EXT_DIR)
            if should_skip(rel_path):
                continue
            if rel_path == Path("manifest.json"):
                archive.writestr(
                    "manifest.json",
                    json.dumps(manifest, indent=2, ensure_ascii=True) + "\n",
                )
                continue
            archive.write(source_path, rel_path.as_posix())

    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a browser-targeted Lattice extension package.",
    )
    parser.add_argument(
        "--browser",
        choices=("firefox", "chrome"),
        required=True,
        help="Target browser packaging profile.",
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Version string used in the output filename.",
    )
    parser.add_argument(
        "--out-dir",
        default="release-assets",
        help="Directory to place the built archive in.",
    )
    args = parser.parse_args()

    out_path = write_zip(args.browser, args.version, (REPO_ROOT / args.out_dir).resolve())
    print(out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
