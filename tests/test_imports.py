import sys
from pathlib import Path
import unittest

src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

import fabric_perf


class TestImports(unittest.TestCase):
    def test_package_metadata(self):
        self.assertTrue(hasattr(fabric_perf, "__version__"))
        self.assertEqual(fabric_perf.__version__, "0.1.0")
        self.assertIn("Aryan Prasad", fabric_perf.__author__)


if __name__ == "__main__":
    unittest.main()
