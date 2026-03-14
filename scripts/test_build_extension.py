#!/usr/bin/env python3

import json
from pathlib import Path
import tempfile
import unittest
import zipfile


from build_extension import archive_name, write_zip


class BuildExtensionTests(unittest.TestCase):
    def test_chrome_build_strips_gecko_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_path = write_zip("chrome", "0.0.0-test", Path(tmp_dir))
            self.assertEqual(out_path.name, archive_name("chrome", "0.0.0-test"))
            with zipfile.ZipFile(out_path) as archive:
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                self.assertNotIn("browser_specific_settings", manifest)

    def test_firefox_build_keeps_gecko_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_path = write_zip("firefox", "0.0.0-test", Path(tmp_dir))
            with zipfile.ZipFile(out_path) as archive:
                manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
                self.assertIn("browser_specific_settings", manifest)


if __name__ == "__main__":
    unittest.main()
