"""Cubre la rama tls_bad_record_mac de validate_record_logic, que
glue-jobs/tests/test_schema_validation.py no ejercita todavía (quedó
señalado como código inerte sin probar en docs/gobernanza/estado_gobernanza.md
-- "dos de las tres reglas de calidad nunca se han ejercitado"). Colocar
junto a glue-jobs/tests/test_schema_validation.py.
"""
import unittest
from src.schema_validation import validate_record_logic


def _base_valid_record(**overrides):
    record = {
        "frame.time_epoch": "1608227266.862492000",
        "frame.len": "128",
        "wlan.fc.type": "2",
        "wlan.bssid": "00:11:22:33:44:55",
        "wlan.sa": "00:11:22:33:44:55",
        "Label": "Normal",
    }
    record.update(overrides)
    return record


class TestSchemaValidationTls(unittest.TestCase):
    def test_tls_bad_record_mac_is_rejected(self):
        record = _base_valid_record(**{"tls.alert_message.desc": "bad_record_mac"})
        result = validate_record_logic(record)
        self.assertEqual(result, "tls_bad_record_mac")

    def test_tls_other_alert_is_not_rejected(self):
        # Solo bad_record_mac dispara la regla -- cualquier otra alerta TLS no
        # implica datos corruptos para efectos de este pipeline.
        record = _base_valid_record(**{"tls.alert_message.desc": "handshake_failure"})
        result = validate_record_logic(record)
        self.assertEqual(result, "")

    def test_record_without_tls_field_is_not_rejected(self):
        # La mayoría de tramas AWID3 no tienen este campo -- confirmar que su
        # ausencia no rompe la validación (regresión del bug de
        # tls.decryption_failed, columna que no existía en el esquema real).
        record = _base_valid_record()
        self.assertNotIn("tls.alert_message.desc", record)
        result = validate_record_logic(record)
        self.assertEqual(result, "")


if __name__ == "__main__":
    unittest.main()
