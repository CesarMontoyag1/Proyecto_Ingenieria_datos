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


def test_record_without_tls_field_is_not_rejected():
    # La mayoría de tramas AWID3 no tienen este campo: su ausencia no debe
    # romper la validación.
    record = _base_valid_record()
    assert "tls.alert_message.desc" not in record
    result = validate_record_logic(record)
    assert result == ""


def test_tls_bad_record_mac_also_applies_to_control_frames():
    # La regla no depende del tipo de trama: una trama de control (tipo 1)
    # con bad_record_mac también se rechaza, aunque no tenga wlan.bssid.
    record = {
        "frame.time_epoch": "1608227266.862492000",
        "frame.len": "14",
        "wlan.fc.type": "1",
        "wlan.ra": "00:11:22:33:44:55",
        "Label": "Normal",
        "tls.alert_message.desc": "bad_record_mac",
    }
    assert validate_record_logic(record) == "tls_bad_record_mac"


def test_missing_critical_column_takes_precedence_over_tls_rule():
    # Si falta una columna universal, el motivo registrado es la columna
    # ausente, no el error TLS: así el motivo en Quarantine es el primero
    # que impide aceptar la fila.
    record = _base_valid_record(**{"tls.alert_message.desc": "bad_record_mac"})
    del record["frame.len"]
    assert validate_record_logic(record) == "missing_critical_column:frame.len"
if __name__ == "__main__":
    unittest.main()
