import json
import os
import unittest
from unittest.mock import MagicMock

from src.alert_formatter.handler import format_alert_message, lambda_handler


class TestAlertFormatter(unittest.TestCase):
    def setUp(self):
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:nexasecure-soc-alerts-dev"

    def test_format_alert_message(self):
        alert = {
            "rule_type": "DEAUTH_FLOOD",
            "severity": "HIGH",
            "bssid": "00:11:22:aabbcc",
            "src_mac": "00:11:22:112233",
            "observed_count": 15,
            "threshold": 10,
            "window_seconds": 5.0,
            "timestamp": 1600000000.0,
            "details": {"reason": "Test deauth burst"}
        }
        msg = format_alert_message(alert)
        self.assertIn("DEAUTH_FLOOD", msg)
        self.assertIn("HIGH", msg)
        self.assertIn("00:11:22:aabbcc", msg)
        self.assertIn("15", msg)

    def test_lambda_handler_success(self):
        mock_sns = MagicMock()

        sample_alert = {
            "rule_type": "DEAUTH_FLOOD",
            "severity": "HIGH",
            "bssid": "0c:9d:92:ad2596f1",
            "src_mac": "74:d0:2b:d906e028",
            "observed_count": 12,
            "threshold": 10,
            "window_seconds": 5.0,
            "timestamp": 1608323755.0
        }

        event = {
            "Records": [
                {
                    "messageId": "msg-001",
                    "body": json.dumps(sample_alert)
                }
            ]
        }

        result = lambda_handler(event, None, sns_client=mock_sns)

        # Batch report failures must be empty
        self.assertEqual(result["batchItemFailures"], [])
        # SNS publish must be called once
        self.assertEqual(mock_sns.publish.call_count, 1)
        call_kwargs = mock_sns.publish.call_args[1]
        self.assertEqual(call_kwargs["TopicArn"], "arn:aws:sns:us-east-1:123456789012:nexasecure-soc-alerts-dev")
        self.assertIn("[NexaSecure WIDS - HIGH] DEAUTH_FLOOD", call_kwargs["Subject"])
        self.assertIn("DEAUTH_FLOOD", call_kwargs["Message"])

    def test_lambda_handler_handles_malformed_record(self):
        mock_sns = MagicMock()

        event = {
            "Records": [
                {
                    "messageId": "bad-msg-1",
                    "body": "{invalid-json}"
                },
                {
                    "messageId": "bad-msg-2",
                    "body": json.dumps({"rule_type": "INCOMPLETE"}) # Missing severity, bssid, timestamp
                }
            ]
        }

        result = lambda_handler(event, None, sns_client=mock_sns)

        # Both bad messages must be reported in batchItemFailures
        failed_ids = [item["itemIdentifier"] for item in result["batchItemFailures"]]
        self.assertIn("bad-msg-1", failed_ids)
        self.assertIn("bad-msg-2", failed_ids)
        self.assertEqual(mock_sns.publish.call_count, 0)


if __name__ == "__main__":
    unittest.main()

