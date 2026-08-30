import json
from typing import Dict, Any
from src.awid3_schema import AWID3_CRITICAL_COLUMNS

def validate_record_logic(record: Dict[str, Any]) -> str:
    """
    Returns an empty string if valid, or a rejection reason if invalid.
    """
    # 1. Check for missing critical columns
    for col in AWID3_CRITICAL_COLUMNS:
        if col not in record or record[col] is None or str(record[col]).strip() == "":
            return f"missing_critical_column:{col}"

    # 2. Check for TCP payload overflow (known AWID3 dataset issue)
    if "tcp.payload" in record and record["tcp.payload"]:
        payload = str(record["tcp.payload"])
        if len(payload) > 32767:
            return "tcp_payload_overflow"

    # 3. Check for TLS decryption errors
    if record.get("tls.decryption_failed") == "1" or record.get("tls.alert_message.desc") == "bad_record_mac":
        return "tls_decryption_error"

    return ""

