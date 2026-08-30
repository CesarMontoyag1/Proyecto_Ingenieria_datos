from dataclasses import dataclass
from typing import Dict, Any, Optional
from src.awid3_schema import AWID3_CRITICAL_COLUMNS

@dataclass
class ValidationResult:
    is_valid: bool
    rejection_reason: Optional[str] = None

def validate_record(record: Dict[str, Any]) -> ValidationResult:
    # 1. Check for missing critical columns
    for col in AWID3_CRITICAL_COLUMNS:
        if col not in record or record[col] == "":
            return ValidationResult(is_valid=False, rejection_reason=f"missing_critical_column:{col}")

    # 2. Check for TCP payload overflow (known AWID3 dataset issue)
    # The dataset has an overflow where payload length doesn't match actual string length
    if "tcp.payload" in record and record["tcp.payload"]:
        payload = str(record["tcp.payload"])
        # Simple heuristic: if payload is longer than max expected size, mark invalid
        if len(payload) > 32767:
            return ValidationResult(is_valid=False, rejection_reason="tcp_payload_overflow")

    # 3. Check for TLS decryption errors
    # (Assuming there's a column 'tls.decryption_failed' based on docs)
    if record.get("tls.decryption_failed") == "1" or record.get("tls.alert_message.desc") == "bad_record_mac":
        return ValidationResult(is_valid=False, rejection_reason="tls_decryption_error")

    return ValidationResult(is_valid=True)

