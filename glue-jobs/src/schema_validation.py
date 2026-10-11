import json
from typing import Dict, Any
from src.awid3_schema import UNIVERSAL_CRITICAL_COLUMNS

def _clean_str(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s

def validate_record_logic(record: Dict[str, Any]) -> str:
    """
    Returns an empty string if valid, or a rejection reason if invalid.
    Calibrated for 802.11 protocol: Control frames (type 1) omit BSSID by design.
    """
    # 1. Check universal critical columns present in all 802.11 frames
    for col in UNIVERSAL_CRITICAL_COLUMNS:
        if col not in record or record[col] is None or _clean_str(record[col]) == "":
            return f"missing_critical_column:{col}"

    # 2. Conditional Wi-Fi frame type validation
    # wlan.fc.type: 0 = Management, 1 = Control, 2 = Data
    frame_type = _clean_str(record.get("wlan.fc.type"))

    if frame_type == "1":
        # Control frames (ACK, CTS, RTS): do NOT contain wlan.bssid by 802.11 design.
        # Must have at least receiver (wlan.ra) or destination (wlan.da) address
        ra = _clean_str(record.get("wlan.ra"))
        da = _clean_str(record.get("wlan.da"))
        if not (ra or da):
            return "missing_receiver_address_in_control_frame"
    else:
        # Management (0) and Data (2) frames require BSSID and source/transmitter address
        bssid = _clean_str(record.get("wlan.bssid"))
        sa = _clean_str(record.get("wlan.sa"))
        ta = _clean_str(record.get("wlan.ta"))
        if not bssid:
            return "missing_bssid_in_data_or_mgmt_frame"
        if not (sa or ta):
            return "missing_source_address"

    # 3. Check for TCP payload overflow (known AWID3 dataset issue)
    if "tcp.payload" in record and record["tcp.payload"]:
        payload = str(record["tcp.payload"])
        if len(payload) > 32767:
            return "tcp_payload_overflow"

    # 4. Check for TLS bad record MAC
    if record.get("tls.alert_message.desc") == "bad_record_mac":
        return "tls_bad_record_mac"

    return ""

