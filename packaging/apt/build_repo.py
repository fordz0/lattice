#!/usr/bin/env python3

from __future__ import annotations

import argparse
import gzip
import hashlib
import math
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class PackageInfo:
    arch: str
    deb_filename: str
    pool_filename: str
    deb_path: Path
    size: int
    md5: str
    sha256: str
    installed_size_kib: int


PACKAGE_NAME = "lattice"
DESCRIPTION_SHORT = "Peer-to-peer web protocol CLI and daemon"
DESCRIPTION_LONG = (
    " Publish and access .loom sites without DNS, registrars, or a central host.\n"
    " .\n"
    " Includes the lattice CLI, lattice-daemon, and a user systemd service file."
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def md5_file(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_release_tree(tarball: Path, work_dir: Path) -> Path:
    extract_dir = work_dir / "extract"
    extract_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tarball, "r:gz") as archive:
        archive.extractall(extract_dir)

    candidates = [
        path for path in extract_dir.iterdir() if path.is_dir() and (path / "lattice").exists()
    ]
    if candidates:
        return candidates[0]
    if (extract_dir / "lattice").exists():
        return extract_dir
    raise FileNotFoundError(f"could not locate extracted release root in {tarball}")


def package_installed_size(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return max(1, math.ceil(total / 1024))


def write_control_file(path: Path, version: str, arch: str, installed_size_kib: int) -> None:
    control = "\n".join(
        [
            f"Package: {PACKAGE_NAME}",
            f"Version: {version}",
            "Section: net",
            "Priority: optional",
            f"Architecture: {arch}",
            "Maintainer: Ben Ford <ben@lattice.local>",
            "Homepage: https://lattice.benjf.dev",
            f"Installed-Size: {installed_size_kib}",
            "Depends:",
            f"Description: {DESCRIPTION_SHORT}",
            DESCRIPTION_LONG,
            "",
        ]
    )
    path.write_text(control, encoding="utf-8")


def create_deb(
    tarball: Path, arch: str, version: str, output_dir: Path, repo_root: Path, service_file: Path
) -> PackageInfo:
    deb_version = f"{version}-1"
    package_filename = f"{PACKAGE_NAME}_{deb_version}_{arch}.deb"
    package_path = output_dir / package_filename

    with tempfile.TemporaryDirectory(prefix=f"lattice-deb-{arch}-") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        release_root = extract_release_tree(tarball, temp_dir)
        pkg_root = temp_dir / "pkgroot"
        debian_dir = pkg_root / "DEBIAN"
        debian_dir.mkdir(parents=True, exist_ok=True)

        usr_bin = pkg_root / "usr" / "bin"
        usr_bin.mkdir(parents=True, exist_ok=True)
        shutil.copy2(release_root / "lattice", usr_bin / "lattice")
        shutil.copy2(release_root / "lattice-daemon", usr_bin / "lattice-daemon")

        doc_dir = pkg_root / "usr" / "share" / "doc" / PACKAGE_NAME
        doc_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(release_root / "README.md", doc_dir / "README.md")
        shutil.copy2(release_root / "LICENSE", doc_dir / "copyright")

        systemd_user_dir = pkg_root / "usr" / "lib" / "systemd" / "user"
        systemd_user_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(service_file, systemd_user_dir / "lattice-daemon.service")

        installed_size_kib = package_installed_size(pkg_root)
        write_control_file(debian_dir / "control", deb_version, arch, installed_size_kib)

        subprocess.run(
            ["dpkg-deb", "--build", "--root-owner-group", str(pkg_root), str(package_path)],
            check=True,
        )

    pool_relpath = Path("pool") / "main" / "l" / PACKAGE_NAME / package_filename
    pool_dest = repo_root / pool_relpath
    pool_dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(package_path, pool_dest)

    return PackageInfo(
        arch=arch,
        deb_filename=package_filename,
        pool_filename=pool_relpath.as_posix(),
        deb_path=package_path,
        size=package_path.stat().st_size,
        md5=md5_file(package_path),
        sha256=sha256_file(package_path),
        installed_size_kib=installed_size_kib,
    )


def write_packages(repo_root: Path, package: PackageInfo, version: str) -> None:
    binary_dir = repo_root / "dists" / "stable" / "main" / f"binary-{package.arch}"
    binary_dir.mkdir(parents=True, exist_ok=True)
    deb_version = f"{version}-1"
    content = "\n".join(
        [
            f"Package: {PACKAGE_NAME}",
            f"Version: {deb_version}",
            f"Architecture: {package.arch}",
            "Section: net",
            "Priority: optional",
            "Maintainer: Ben Ford <ben@lattice.local>",
            "Homepage: https://lattice.benjf.dev",
            f"Installed-Size: {package.installed_size_kib}",
            f"Filename: {package.pool_filename}",
            f"Size: {package.size}",
            f"MD5sum: {package.md5}",
            f"SHA256: {package.sha256}",
            f"Description: {DESCRIPTION_SHORT}",
            DESCRIPTION_LONG,
            "",
        ]
    )
    packages_path = binary_dir / "Packages"
    packages_path.write_text(content, encoding="utf-8")
    with gzip.open(binary_dir / "Packages.gz", "wb") as handle:
        handle.write(content.encode("utf-8"))


def file_hash(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_release(repo_root: Path) -> None:
    release_dir = repo_root / "dists" / "stable"
    entries = []
    for rel_path in [
        Path("main/binary-amd64/Packages"),
        Path("main/binary-amd64/Packages.gz"),
        Path("main/binary-arm64/Packages"),
        Path("main/binary-arm64/Packages.gz"),
    ]:
        abs_path = release_dir / rel_path
        entries.append(
            (
                rel_path.as_posix(),
                abs_path.stat().st_size,
                file_hash(abs_path, "md5"),
                file_hash(abs_path, "sha256"),
            )
        )

    release_lines = [
        "Origin: Lattice",
        "Label: Lattice",
        "Suite: stable",
        "Codename: stable",
        "Version: stable",
        "Architectures: amd64 arm64",
        "Components: main",
        f"Date: {datetime.now(timezone.utc):%a, %d %b %Y %H:%M:%S %z}",
        "MD5Sum:",
    ]
    for rel_path, size, md5, _sha256 in entries:
        release_lines.append(f" {md5} {size:16d} {rel_path}")
    release_lines.append("SHA256:")
    for rel_path, size, _md5, sha256 in entries:
        release_lines.append(f" {sha256} {size:16d} {rel_path}")
    release_lines.append("")

    (release_dir / "Release").write_text("\n".join(release_lines), encoding="utf-8")


def archive_repo(repo_root: Path, output_dir: Path) -> Path:
    archive_path = output_dir / "lattice-apt-repo.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(repo_root, arcname="lattice-apt-repo")
    return archive_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build Debian packages and an APT repo snapshot from release tarballs."
    )
    parser.add_argument("--version", required=True, help="Release version without prefix")
    parser.add_argument("--amd64-tarball", required=True, type=Path)
    parser.add_argument("--arm64-tarball", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    repo_root = output_dir / "lattice-apt-repo"
    if repo_root.exists():
        shutil.rmtree(repo_root)
    repo_root.mkdir(parents=True, exist_ok=True)

    service_file = (
        Path(__file__).resolve().parents[1]
        / "aur"
        / "lattice-net-git"
        / "lattice-daemon.service"
    )

    amd64_package = create_deb(
        args.amd64_tarball.resolve(), "amd64", args.version, output_dir, repo_root, service_file
    )
    arm64_package = create_deb(
        args.arm64_tarball.resolve(), "arm64", args.version, output_dir, repo_root, service_file
    )

    write_packages(repo_root, amd64_package, args.version)
    write_packages(repo_root, arm64_package, args.version)
    write_release(repo_root)
    archive_repo(repo_root, output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
