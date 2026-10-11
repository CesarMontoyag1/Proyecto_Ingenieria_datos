from dataclasses import dataclass
from typing import Any, Dict, Optional

UNIVERSAL_CRITICAL_COLUMNS = [
    "frame.time_epoch",
    "frame.len",
    "Label"
]

@dataclass
class ValidationResult:
    is_valid: bool
    rejection_reason: Optional[str] = None


def _clean_str(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def validate_record(record: Dict[str, Any]) -> ValidationResult:
    """
    Valida la calidad del registro según el protocolo 802.11.
    Tramas de Control (type=1) no exigen BSSID por diseño de estándar Wi-Fi.
    """
    # 1. Columnas críticas universales
    for col in UNIVERSAL_CRITICAL_COLUMNS:
        if col not in record or record[col] is None or _clean_str(record[col]) == "":
            return ValidationResult(is_valid=False, rejection_reason=f"missing_critical_column:{col}")

    # 2. Validación condicional por tipo de trama 802.11
    # wlan.fc.type: 0 = Management, 1 = Control, 2 = Data
    frame_type = _clean_str(record.get("wlan.fc.type"))

    if frame_type == "1":
        # Tramas de control (ACK, CTS, RTS): no llevan BSSID. Requieren receptor (RA) o destino (DA).
        ra = _clean_str(record.get("wlan.ra"))
        da = _clean_str(record.get("wlan.da"))
        if not (ra or da):
            return ValidationResult(is_valid=False, rejection_reason="missing_receiver_address_in_control_frame")
    else:
        # Tramas de gestión (0) y datos (2) requieren BSSID y dirección origen
        bssid = _clean_str(record.get("wlan.bssid"))
        sa = _clean_str(record.get("wlan.sa"))
        ta = _clean_str(record.get("wlan.ta"))
        if not bssid:
            return ValidationResult(is_valid=False, rejection_reason="missing_bssid_in_data_or_mgmt_frame")
        if not (sa or ta):
            return ValidationResult(is_valid=False, rejection_reason="missing_source_address")

    # 3. Control de desbordamiento en tcp.payload (defecto conocido del dataset AWID3)
    if "tcp.payload" in record and record["tcp.payload"]:
        payload = str(record["tcp.payload"])
        if len(payload) > 32767:
            return ValidationResult(is_valid=False, rejection_reason="tcp_payload_overflow")

    # 4. Error de registro TLS
    if record.get("tls.alert_message.desc") == "bad_record_mac":
        return ValidationResult(is_valid=False, rejection_reason="tls_bad_record_mac")

    return ValidationResult(is_valid=True)
