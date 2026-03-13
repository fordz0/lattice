from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class ReleasePackagingTests(unittest.TestCase):
    def test_debian_postinst_runs_restart_helper(self) -> None:
        build_repo = load_module(
            "build_repo", REPO_ROOT / "packaging" / "apt" / "build_repo.py"
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            postinst_path = Path(temp_dir) / "postinst"
            build_repo.write_postinst(postinst_path)

            content = postinst_path.read_text(encoding="utf-8")
            self.assertIn('/usr/lib/lattice/restart-daemon-if-active.sh', content)
            self.assertIn('configure|triggered', content)
            self.assertTrue(postinst_path.stat().st_mode & 0o111)

    def test_homebrew_formula_includes_restart_logic(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            x86_sha = temp_path / "x86.sha256"
            arm_sha = temp_path / "arm.sha256"
            output = temp_path / "lattice-net.rb"

            x86_sha.write_text("deadbeef  lattice-macos-x86_64.tar.gz\n", encoding="utf-8")
            arm_sha.write_text(
                "feedface  lattice-macos-aarch64.tar.gz\n", encoding="utf-8"
            )

            subprocess.run(
                [
                    "python3",
                    str(REPO_ROOT / "packaging" / "homebrew" / "lattice-net" / "render_formula.py"),
                    "--version",
                    "0.1.6",
                    "--macos-x86-sha256",
                    str(x86_sha),
                    "--macos-arm64-sha256",
                    str(arm_sha),
                    "--output",
                    str(output),
                ],
                check=True,
                cwd=REPO_ROOT,
            )

            content = output.read_text(encoding="utf-8")
            self.assertIn('def post_install', content)
            self.assertIn('launchctl", "kickstart", "-k"', content)
            self.assertIn('gui/#{Process.uid}/#{plist_name}', content)
            self.assertIn('system/#{plist_name}', content)


if __name__ == "__main__":
    unittest.main()
