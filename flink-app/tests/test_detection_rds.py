import unittest
from unittest.mock import MagicMock
from src.detection_job import insert_alert_to_rds
from src.detection_rules import DetectionEngine

TEST_KEY = b"test_secret_key_1234567890_64_bytes_padding_required_for_testing"


class TestDetectionRds(unittest.TestCase):
    def test_insert_alert_to_rds_success(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        alert = {
            "alert_id": "test-uuid-1234",
            "rule_type": "DEAUTH_FLOOD",
            "severity": "HIGH",
            "bssid": "00:14:6c:abcd1234",  # Already pseudonymized
            "src_mac": "12:34:56:fedc9876",  # Already pseudonymized
            "observed_count": 15,
            "threshold": 10,
            "window_seconds": 5.0,
            "timestamp": 1608227266.0,
            "details": {"reason": "Test flood"}
        }

        success = insert_alert_to_rds(mock_conn, alert)
        self.assertTrue(success)
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

        call_args = mock_cursor.execute.call_args[0]
        sql_query = call_args[0]
        sql_params = call_args[1]

        self.assertIn("INSERT INTO alerts", sql_query)
        self.assertEqual(sql_params[0], "DEAUTH_FLOOD")
        self.assertEqual(sql_params[1], "HIGH")
        self.assertEqual(sql_params[2], "00:14:6c:abcd1234")  # bssid_pseudo
        self.assertEqual(sql_params[3], "12:34:56:fedc9876")  # src_mac_pseudo
        self.assertEqual(sql_params[4], 15)  # observed_count
        self.assertEqual(sql_params[5], 10)  # threshold
        self.assertEqual(sql_params[6], 5)   # window_seconds

    def test_insert_alert_to_rds_handles_db_error_gracefully(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("DB connection dropped")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        alert = {
            "rule_type": "ROGUE_AP",
            "severity": "CRITICAL",
            "bssid": "00:14:6c:abcd1234",
            "timestamp": 1608227266.0
        }

        # Must not raise exception, returns False gracefully
        success = insert_alert_to_rds(mock_conn, alert)
        self.assertFalse(success)
        mock_conn.rollback.assert_called_once()

    def test_load_rules_from_rds(self):
        engine = DetectionEngine(hmac_key=TEST_KEY)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("DEAUTH_FLOOD", "HIGH", 12, 6),
            ("DISAS_BURST", "MEDIUM", 15, 7)
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        recalibrated = engine.load_rules_from_rds(mock_conn)
        self.assertTrue(recalibrated)
        self.assertEqual(engine.deauth_threshold, 12)
        self.assertEqual(engine.deauth_window_sec, 6.0)
        self.assertEqual(engine.disas_threshold, 15)
        self.assertEqual(engine.disas_window_sec, 7.0)


if __name__ == "__main__":
    unittest.main()

