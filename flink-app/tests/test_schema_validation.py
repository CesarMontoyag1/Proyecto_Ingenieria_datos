import unittest
from src.schema_validation import validate_record


class TestSchemaValidation(unittest.TestCase):
    def test_validate_record_valid_data_frame(self):
        record = {
            "frame.time_epoch": "1608227266.862492000",
            "frame.len": "128",
            "wlan.fc.type": "2",  # Data frame
            "wlan.bssid": "00:11:22:33:44:55",
            "wlan.sa": "00:11:22:33:44:55",
            "wlan.da": "ff:ff:ff:ff:ff:ff",
            "Label": "Normal"
        }
        result = validate_record(record)
        self.assertTrue(result.is_valid)

    def test_validate_record_valid_control_frame_without_bssid(self):
        # Tramas de control 802.11 (ACK, RTS, CTS): type=1, no tienen BSSID por estándar
        record = {
            "frame.time_epoch": "1608227266.862492000",
            "frame.len": "14",
            "wlan.fc.type": "1",  # Control frame
            "wlan.ra": "00:11:22:33:44:55",  # Receiver address
            "Label": "Normal"
        }
        result = validate_record(record)
        self.assertTrue(result.is_valid, f"Control frame should be valid but got {result.rejection_reason}")

    def test_validate_record_missing_critical(self):
        record = {
            "frame.len": "128",
            "wlan.bssid": "00:11:22:33:44:55",
            "Label": "Normal"
        }
        result = validate_record(record)
        self.assertFalse(result.is_valid)
        self.assertIn("missing_critical_column", result.rejection_reason)

    def test_validate_record_tcp_payload_overflow(self):
        record = {
            "frame.time_epoch": "1608227266",
            "frame.len": "128",
            "wlan.fc.type": "2",
            "wlan.bssid": "00:11:22:33:44:55",
            "wlan.sa": "00:11:22:33:44:55",
            "wlan.da": "ff:ff:ff:ff:ff:ff",
            "Label": "Normal",
            "tcp.payload": "A" * 32768
        }
        result = validate_record(record)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.rejection_reason, "tcp_payload_overflow")


if __name__ == "__main__":
    unittest.main()
