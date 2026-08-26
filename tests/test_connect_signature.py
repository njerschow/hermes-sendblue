import inspect
import unittest
from adapter import SendblueAdapter

class ConnectSignatureTests(unittest.TestCase):
    def test_connect_accepts_is_reconnect_keyword(self):
        parameter = inspect.signature(SendblueAdapter.connect).parameters.get("is_reconnect")
        self.assertIsNotNone(parameter)
        self.assertEqual(parameter.kind, inspect.Parameter.KEYWORD_ONLY)
        self.assertFalse(parameter.default)

if __name__ == "__main__":
    unittest.main()
