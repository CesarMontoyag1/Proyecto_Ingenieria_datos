import time
import unittest
from src.detection_rules import DetectionEngine
from src.pseudonymize import pseudonymize_mac

TEST_KEY = b"test_hmac_secret_key_1234567890_64_bytes_padding_here_for_testing"


class TestDetectionRules(unittest.TestCase):
    def setUp(self):
        self.engine = DetectionEngine(
            hmac_key=TEST_KEY,
            deauth_threshold=5,
            deauth_window_sec=5.0,
            disas_threshold=4,
            disas_window_sec=5.0,
            alert_cooldown_sec=60.0,
            ssid_whitelist={
                "NexaSecure-Corp": {"00:11:22:33:44:55"}
            }
        )

    def test_deauth_flood_triggers_when_threshold_reached(self):
        base_time = 1000.0
        target_bssid = "aa:bb:cc:dd:ee:ff"
        attacker_mac = "11:22:33:44:55:66"

        record = {
            "wlan.fc.type": "0",
            "wlan.fc.subtype": "12",
            "wlan.bssid": target_bssid,
            "wlan.sa": attacker_mac,
            "wlan.fixed.reason_code": "7"
        }

        alerts = []
        # Enviar 4 eventos (debajo del umbral de 5)
        for i in range(4):
            alerts.extend(self.engine.process_event({**record, "frame.time_epoch": base_time + (i * 0.5)}))
        self.assertEqual(len(alerts), 0)

        # Enviar el 5to evento dentro de la ventana de 5s -> debe disparar
        alerts.extend(self.engine.process_event({**record, "frame.time_epoch": base_time + 2.5}))
        self.assertEqual(len(alerts), 1)

        alert = alerts[0]
        self.assertEqual(alert["rule_type"], "DEAUTH_FLOOD")
        self.assertEqual(alert["severity"], "HIGH")
        self.assertEqual(alert["observed_count"], 5)
        self.assertEqual(alert["threshold"], 5)

        # Verificar seudonimización de MACs (sin PII en texto plano)
        expected_bssid = pseudonymize_mac(target_bssid, TEST_KEY)
        expected_sa = pseudonymize_mac(attacker_mac, TEST_KEY)
        self.assertEqual(alert["bssid"], expected_bssid)
        self.assertEqual(alert["src_mac"], expected_sa)
        self.assertNotIn(target_bssid, alert["bssid"])

    def test_deauth_deduplication_cooldown(self):
        base_time = 2000.0
        target_bssid = "00:aa:bb:cc:dd:ee"
        record = {
            "wlan.fc.type": "0",
            "wlan.fc.subtype": "12",
            "wlan.bssid": target_bssid,
            "wlan.sa": "ff:ee:dd:cc:bb:aa"
        }

        # Generar primera alerta con 5 tramas
        alerts1 = []
        for i in range(5):
            alerts1.extend(self.engine.process_event({**record, "frame.time_epoch": base_time + i}))
        self.assertEqual(len(alerts1), 1)

        # Ráfaga subsiguiente a los 10 segundos (< cooldown de 60s) -> Debe suprimirse
        alerts2 = []
        for i in range(5):
            alerts2.extend(self.engine.process_event({**record, "frame.time_epoch": base_time + 10 + i}))
        self.assertEqual(len(alerts2), 0)

        # Ráfaga posterior al cooldown de 60s (a los 70s) -> Debe emitir nueva alerta
        alerts3 = []
        for i in range(5):
            alerts3.extend(self.engine.process_event({**record, "frame.time_epoch": base_time + 70 + i}))
        self.assertEqual(len(alerts3), 1)

    def test_rogue_ap_detection(self):
        event_time = 3000.0

        # Caso legítimo: SSID permitido con BSSID en lista blanca
        legit_record = {
            "wlan.ssid": "NexaSecure-Corp",
            "wlan.bssid": "00:11:22:33:44:55",
            "frame.time_epoch": event_time
        }
        alerts = self.engine.process_event(legit_record)
        self.assertEqual(len(alerts), 0)

        # Caso anómalo: SSID permitido pero con BSSID desconocido (Rogue AP / Evil Twin)
        rogue_bssid = "99:88:77:66:55:44"
        rogue_record = {
            "wlan.ssid": "NexaSecure-Corp",
            "wlan.bssid": rogue_bssid,
            "frame.time_epoch": event_time + 1
        }
        alerts = self.engine.process_event(rogue_record)
        self.assertEqual(len(alerts), 1)

        alert = alerts[0]
        self.assertEqual(alert["rule_type"], "ROGUE_AP")
        self.assertEqual(alert["severity"], "HIGH")
        self.assertEqual(alert["bssid"], pseudonymize_mac(rogue_bssid, TEST_KEY))
        self.assertIn("Rogue AP", alert["details"]["reason"])

    def test_disas_burst_detection(self):
        base_time = 4000.0
        sa = "12:34:56:78:9a:bc"
        bssid = "aa:11:bb:22:cc:33"

        record = {
            "wlan.fc.type": "0",
            "wlan.fc.subtype": "10",  # Disas
            "wlan.sa": sa,
            "wlan.bssid": bssid
        }

        alerts = []
        for i in range(4):
            alerts.extend(self.engine.process_event({**record, "frame.time_epoch": base_time + (i * 0.8)}))

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["rule_type"], "DISAS_BURST")
        self.assertEqual(alerts[0]["severity"], "MEDIUM")


if __name__ == "__main__":
    unittest.main()

