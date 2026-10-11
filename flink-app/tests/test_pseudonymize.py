import unittest
from src.pseudonymize import pseudonymize_mac


class TestPseudonymize(unittest.TestCase):
    def test_pseudonymize_mac(self):
        key = b"supersecretkey"
        mac = "00:11:22:33:44:55"
        result = pseudonymize_mac(mac, key)

        self.assertTrue(result.startswith("00:11:22:"))
        self.assertGreater(len(result), 9)
        self.assertNotEqual(result, mac)

    def test_pseudonymize_broadcast_preserved(self):
        key = b"supersecretkey"
        mac = "ff:ff:ff:ff:ff:ff"
        result = pseudonymize_mac(mac, key)
        # Broadcast address must NOT be hashed
        self.assertEqual(result, "ff:ff:ff:ff:ff:ff")

    def test_pseudonymize_invalid_mac(self):
        key = b"supersecretkey"
        mac = "invalid-mac"
        result = pseudonymize_mac(mac, key)

        self.assertNotEqual(result, mac)
        self.assertNotIn(":", result)


if __name__ == "__main__":
    unittest.main()
