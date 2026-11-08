import json
import os
import unittest
from unittest.mock import MagicMock, patch

from src.alert_formatter.handler import build_serving_record, lambda_handler, serving_object_key

ALERT = {
    "alert_id": "3f0c6a8e-1b2c-4d5e-8f90-123456789abc",
    "rule_type": "DEAUTH_FLOOD",
    "severity": "high",
    "bssid": "00:14:6c:abcd1234",
    "src_mac": "12:34:56:fedc9876",
    "observed_count": 15,
    "threshold": 10,
    "window_seconds": 5.0,
    "timestamp": 1794096480.5,  # 2026-11-08T00:08:00.5Z
    "detection_latency_ms": 566.7,
    "published_at": "2026-11-08T00:08:01+00:00",
    "details": {"reason": "Ráfaga", "reason_code": "7"},
}

ENV = {
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:nexasecure-soc-alerts-dev",
    "SERVING_BUCKET": "nexasecure-wids-awid3-trusted-dev-local",
    "SERVING_PREFIX": "serving/alerts",
}


def sqs_event(*bodies):
    return {"Records": [{"messageId": f"msg-{i}", "body": b} for i, b in enumerate(bodies)]}


class TestServingRecord(unittest.TestCase):
    def test_record_is_flat_and_pseudonymized(self):
        record = build_serving_record(dict(ALERT), "msg-0")
        self.assertEqual(record["alert_id"], ALERT["alert_id"])
        self.assertEqual(record["severity"], "HIGH")
        self.assertEqual(record["bssid_pseudo"], "00:14:6c:abcd1234")
        self.assertEqual(record["detected_at"], "2026-11-08T00:08:00.500000+00:00")
        self.assertEqual(record["dt"], "2026-11-08")
        self.assertEqual(json.loads(record["details"])["reason_code"], "7")

    def test_missing_alert_id_uses_message_id_deterministically(self):
        alert = {k: v for k, v in ALERT.items() if k != "alert_id"}
        first = build_serving_record(dict(alert), "msg-42")
        second = build_serving_record(dict(alert), "msg-42")
        self.assertTrue(first["alert_id"].startswith("sqs-"))
        self.assertEqual(first["alert_id"], second["alert_id"])

    def test_unsafe_alert_id_cannot_escape_the_prefix(self):
        record = build_serving_record(dict(ALERT, alert_id="../../raw/x"), "msg-0")
        key = serving_object_key("serving/alerts", record)
        self.assertNotIn("..", key)
        self.assertTrue(key.startswith("serving/alerts/dt=2026-11-08/sqs-"))

    def test_optional_fields_tolerate_bad_values(self):
        record = build_serving_record(dict(ALERT, observed_count="n/a", src_mac="N/A"), "msg-0")
        self.assertIsNone(record["observed_count"])
        self.assertIsNone(record["src_mac_pseudo"])


class TestLambdaWritesServing(unittest.TestCase):
    def test_writes_partitioned_object_then_publishes(self):
        sns, s3 = MagicMock(), MagicMock()
        calls = []
        s3.put_object.side_effect = lambda **kw: calls.append("s3")
        sns.publish.side_effect = lambda **kw: calls.append("sns")

        with patch.dict(os.environ, ENV, clear=False):
            result = lambda_handler(sqs_event(json.dumps(ALERT)), None, sns_client=sns, s3_client=s3)

        self.assertEqual(result["batchItemFailures"], [])
        self.assertEqual(calls, ["s3", "sns"])
        kwargs = s3.put_object.call_args.kwargs
        self.assertEqual(kwargs["Bucket"], ENV["SERVING_BUCKET"])
        self.assertEqual(kwargs["Key"], f"serving/alerts/dt=2026-11-08/{ALERT['alert_id']}.json")
        body = json.loads(kwargs["Body"].decode("utf-8"))
        self.assertNotIn("dt", body)  # la partición va en la ruta
        self.assertEqual(body["rule_type"], "DEAUTH_FLOOD")

    def test_s3_failure_returns_message_and_skips_email(self):
        sns, s3 = MagicMock(), MagicMock()
        s3.put_object.side_effect = Exception("AccessDenied")

        with patch.dict(os.environ, ENV, clear=False):
            result = lambda_handler(sqs_event(json.dumps(ALERT)), None, sns_client=sns, s3_client=s3)

        self.assertEqual(result["batchItemFailures"], [{"itemIdentifier": "msg-0"}])
        sns.publish.assert_not_called()

    def test_without_serving_bucket_only_publishes(self):
        sns, s3 = MagicMock(), MagicMock()
        env = {k: v for k, v in ENV.items() if k != "SERVING_BUCKET"}
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("SERVING_BUCKET", None)
            result = lambda_handler(sqs_event(json.dumps(ALERT)), None, sns_client=sns, s3_client=s3)

        self.assertEqual(result["batchItemFailures"], [])
        s3.put_object.assert_not_called()
        sns.publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()
