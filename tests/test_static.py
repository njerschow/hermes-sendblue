from __future__ import annotations

import py_compile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class StaticTests(unittest.TestCase):
    def test_adapter_compiles(self) -> None:
        py_compile.compile(str(ROOT / "adapter.py"), doraise=True)

    def test_manifest_exists(self) -> None:
        self.assertTrue((ROOT / "plugin.yaml").exists())

    def test_plugin_entrypoint_exists(self) -> None:
        self.assertTrue((ROOT / "__init__.py").exists())


if __name__ == "__main__":
    unittest.main()
