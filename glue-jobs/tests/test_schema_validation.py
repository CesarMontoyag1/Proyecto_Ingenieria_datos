import unittest
try:
    from pyspark.sql.functions import col, struct, udf
    from pyspark.sql.types import StringType
except ImportError:
    col = struct = udf = StringType = None

from src.schema_validation import validate_record_logic
from src.pseudonymize import pseudonymize_mac_logic, get_pseudonymize_udf

class TestSchemaValidation(unittest.TestCase):
    def test_validate_record_logic_valid(self):
        record = {
            "frame.time_epoch": "1608227266.862492000",
            "frame.len": "128",
            "wlan.fc.type": "0",
            "wlan.bssid": "00:11:22:33:44:55",
            "wlan.sa": "00:11:22:33:44:55",
            "wlan.da": "ff:ff:ff:ff:ff:ff",
            "Label": "Normal"
        }
        result = validate_record_logic(record)
        self.assertEqual(result, "")

    def test_validate_record_logic_control_frame_valid(self):
        # ACK / CTS / RTS frame (type 1) has no bssid or sa, only receiver address (da/ra)
        record = {
            "frame.time_epoch": "1608227266.862492000",
            "frame.len": "14",
            "wlan.fc.type": "1",
            "wlan.da": "00:11:22:33:44:55",
            "Label": "Normal"
        }
        result = validate_record_logic(record)
        self.assertEqual(result, "")

    def test_validate_record_logic_control_frame_missing_receiver(self):
        record = {
            "frame.time_epoch": "1608227266.862492000",
            "frame.len": "14",
            "wlan.fc.type": "1",
            "Label": "Normal"
        }
        result = validate_record_logic(record)
        self.assertIn("missing_receiver_address_in_control_frame", result)

    def test_validate_record_logic_data_missing_bssid(self):
        record = {
            "frame.time_epoch": "1608227266.862492000",
            "frame.len": "128",
            "wlan.fc.type": "2",
            "wlan.sa": "00:11:22:33:44:55",
            "Label": "Normal"
        }
        result = validate_record_logic(record)
        self.assertIn("missing_bssid_in_data_or_mgmt_frame", result)

    def test_validate_record_logic_tcp_overflow(self):
        record = {
            "frame.time_epoch": "1608227266.862492000",
            "frame.len": "35000",
            "wlan.fc.type": "2",
            "wlan.bssid": "00:11:22:33:44:55",
            "wlan.sa": "00:11:22:33:44:55",
            "tcp.payload": "A" * 33000,
            "Label": "Normal"
        }
        result = validate_record_logic(record)
        self.assertIn("tcp_payload_overflow", result)

    def test_validate_record_logic_missing_critical(self):
        record = {
            "frame.len": "128",
            "Label": "Normal"
        }
        result = validate_record_logic(record)
        self.assertIn("missing_critical_column", result)

    def test_pseudonymize_mac_logic(self):
        key = b"secret"
        mac = "00:11:22:33:44:55"
        pseudo = pseudonymize_mac_logic(mac, key)
        self.assertTrue(pseudo.startswith("00:11:22:"))
        self.assertGreater(len(pseudo), 9)
        self.assertNotEqual(pseudo, mac)

if __name__ == "__main__":
    unittest.main()

