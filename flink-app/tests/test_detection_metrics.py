import time
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from src.detection_job import MetricsCollector, kinesis_arrival_epoch, process_event_record

TEST_KEY = b"test_secret_key_1234567890_64_bytes_padding_required_for_testing"

VALID_DEAUTH = {
    "frame.time_epoch": "1608227266.862492000",  # AWID3 es de 2020: no sirve para medir latencia
    "frame.len": 38,
    "Label": "Deauth",
    "wlan.fc.type": "0",
    "wlan.fc.subtype": "12",
    "wlan.bssid": "00:14:6c:7e:40:80",
    "wlan.sa": "12:34:56:78:9a:bc",
}

INVALID_RECORD = {"frame.len": 100, "Label": "Normal"}


def engine_returning(alerts):
    engine = MagicMock()
    engine.process_event.return_value = alerts
    return engine


def deauth_alert():
    return {"rule_type": "DEAUTH_FLOOD", "severity": "HIGH", "bssid": "00:14:6c:abcd1234"}


def datums(data, name, dims=None):
    return [d for d in data if d["MetricName"] == name
            and (dims is None or {x["Name"]: x["Value"] for x in d["Dimensions"]} == dims)]


class TestKinesisArrivalEpoch(unittest.TestCase):
    def test_aware_datetime(self):
        arrival = datetime(2026, 11, 7, 12, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(kinesis_arrival_epoch({"ApproximateArrivalTimestamp": arrival}), arrival.timestamp())

    def test_naive_datetime_is_treated_as_utc(self):
        naive = datetime(2026, 11, 7, 12, 0, 0)
        expected = naive.replace(tzinfo=timezone.utc).timestamp()
        self.assertEqual(kinesis_arrival_epoch({"ApproximateArrivalTimestamp": naive}), expected)

    def test_missing_or_invalid_value(self):
        self.assertIsNone(kinesis_arrival_epoch({}))
        self.assertIsNone(kinesis_arrival_epoch({"ApproximateArrivalTimestamp": "no-es-fecha"}))


class TestLatencyFromKinesisArrival(unittest.TestCase):
    def test_latency_uses_ingest_time_not_dataset_timestamp(self):
        sqs = MagicMock()
        metrics = MetricsCollector(stage="dev")
        ingest_time = time.time() - 1.5  # el evento llegó a Kinesis hace 1,5 s

        is_valid, _, alerts = process_event_record(
            dict(VALID_DEAUTH), engine_returning([deauth_alert()]), TEST_KEY,
            alerts_queue_url="https://sqs.example/queue", sqs_client=sqs,
            ingest_time=ingest_time, metrics=metrics,
        )

        self.assertTrue(is_valid)
        latency = alerts[0]["detection_latency_ms"]
        # Con el timestamp de 2020 la latencia sería de años; con la llegada a Kinesis es ~1.500 ms.
        self.assertGreaterEqual(latency, 1500)
        self.assertLess(latency, 3000)
        self.assertEqual(metrics.latencies_ms, [round(latency, 2)])
        sqs.send_message.assert_called_once()

    def test_without_ingest_time_latency_is_only_processing_time(self):
        sqs = MagicMock()
        _, _, alerts = process_event_record(
            dict(VALID_DEAUTH), engine_returning([deauth_alert()]), TEST_KEY,
            alerts_queue_url="https://sqs.example/queue", sqs_client=sqs,
        )
        self.assertLess(alerts[0]["detection_latency_ms"], 1000)


class TestMetricsCounting(unittest.TestCase):
    def test_counts_processed_quarantined_and_alerts(self):
        metrics = MetricsCollector(stage="dev")
        sqs = MagicMock()

        process_event_record(dict(INVALID_RECORD), engine_returning([]), TEST_KEY, metrics=metrics)
        process_event_record(dict(VALID_DEAUTH), engine_returning([]), TEST_KEY, metrics=metrics)
        process_event_record(
            dict(VALID_DEAUTH), engine_returning([deauth_alert()]), TEST_KEY,
            alerts_queue_url="https://sqs.example/queue", sqs_client=sqs, metrics=metrics,
        )

        self.assertEqual(metrics.events_processed, 3)
        self.assertEqual(metrics.events_quarantined, 1)
        self.assertEqual(metrics.alerts_by_rule, {"DEAUTH_FLOOD": 1})
        self.assertEqual(len(metrics.latencies_ms), 1)

    def test_alert_counted_even_if_sqs_fails(self):
        metrics = MetricsCollector(stage="dev")
        sqs = MagicMock()
        sqs.send_message.side_effect = Exception("SQS no disponible")

        process_event_record(
            dict(VALID_DEAUTH), engine_returning([deauth_alert()]), TEST_KEY,
            alerts_queue_url="https://sqs.example/queue", sqs_client=sqs, metrics=metrics,
        )

        self.assertEqual(metrics.alerts_by_rule, {"DEAUTH_FLOOD": 1})
        self.assertEqual(metrics.latencies_ms, [])  # sin publicación no hay latencia que medir


class TestMetricsCollectorFlush(unittest.TestCase):
    def test_metric_data_matches_dashboard_contract(self):
        metrics = MetricsCollector(stage="dev")
        for _ in range(4):
            metrics.record_event()
        metrics.record_quarantined()
        metrics.record_alert("DEAUTH_FLOOD", 120.0)
        metrics.record_alert("ROGUE_AP", 80.5)

        data = metrics.build_metric_data()
        stage = {"Stage": "dev"}
        self.assertEqual(datums(data, "EventsProcessed", stage)[0]["Value"], 4.0)
        self.assertEqual(datums(data, "EventsQuarantined", stage)[0]["Value"], 1.0)
        self.assertEqual(datums(data, "AlertsGenerated", stage)[0]["Value"], 2.0)
        self.assertEqual(
            datums(data, "AlertsGenerated", {"Stage": "dev", "RuleType": "ROGUE_AP"})[0]["Value"], 1.0)
        latency = datums(data, "DetectionLatencyMs", stage)[0]
        self.assertEqual(latency["Values"], [120.0, 80.5])
        self.assertEqual(latency["Unit"], "Milliseconds")

    def test_zero_counts_are_published_as_heartbeat(self):
        data = MetricsCollector(stage="dev").build_metric_data()
        self.assertEqual(datums(data, "EventsProcessed")[0]["Value"], 0.0)
        self.assertEqual(datums(data, "DetectionLatencyMs"), [])

    def test_latencies_are_split_in_chunks_of_150(self):
        metrics = MetricsCollector(stage="dev")
        for i in range(320):
            metrics.record_alert("DEAUTH_FLOOD", float(i))
        chunks = datums(metrics.build_metric_data(), "DetectionLatencyMs")
        self.assertEqual([len(c["Values"]) for c in chunks], [150, 150, 20])

    def test_flush_publishes_resets_and_respects_call_limit(self):
        metrics = MetricsCollector(stage="dev", namespace="NexaSecure/WIDS")
        for i in range(25):
            metrics.record_alert(f"RULE_{i}", 10.0)
        cloudwatch = MagicMock()

        self.assertTrue(metrics.flush(cloudwatch))

        calls = cloudwatch.put_metric_data.call_args_list
        self.assertTrue(all(len(c.kwargs["MetricData"]) <= 20 for c in calls))
        self.assertTrue(all(c.kwargs["Namespace"] == "NexaSecure/WIDS" for c in calls))
        self.assertEqual(metrics.events_processed, 0)
        self.assertEqual(metrics.alerts_by_rule, {})

    def test_flush_failure_does_not_raise(self):
        metrics = MetricsCollector(stage="dev")
        metrics.record_event()
        cloudwatch = MagicMock()
        cloudwatch.put_metric_data.side_effect = Exception("AccessDenied")

        self.assertFalse(metrics.flush(cloudwatch))
        self.assertEqual(metrics.events_processed, 0)

    def test_disabled_collector_does_not_call_cloudwatch(self):
        metrics = MetricsCollector(stage="dev", enabled=False)
        metrics.record_event()
        cloudwatch = MagicMock()

        self.assertFalse(metrics.flush(cloudwatch))
        cloudwatch.put_metric_data.assert_not_called()


if __name__ == "__main__":
    unittest.main()
