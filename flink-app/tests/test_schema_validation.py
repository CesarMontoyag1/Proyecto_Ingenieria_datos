import pytest
from src.schema_validation import validate_record

def test_validate_record_valid():
    record = {
        "frame.time_epoch": "1608227266.862492000",
        "frame.len": "128",
        "wlan.bssid": "00:11:22:33:44:55",
        "wlan.sa": "00:11:22:33:44:55",
        "wlan.da": "ff:ff:ff:ff:ff:ff",
        "Label": "Normal"
    }
    result = validate_record(record)
    assert result.is_valid == True

def test_validate_record_missing_critical():
    record = {
        "frame.len": "128",
        "wlan.bssid": "00:11:22:33:44:55",
        "Label": "Normal"
    }
    result = validate_record(record)
    assert result.is_valid == False
    assert "missing_critical_column" in result.rejection_reason

def test_validate_record_tcp_payload_overflow():
    record = {
        "frame.time_epoch": "1608227266",
        "frame.len": "128",
        "wlan.bssid": "00:11:22",
        "wlan.sa": "00:11:22",
        "wlan.da": "ff:ff:ff",
        "Label": "Normal",
        "tcp.payload": "A" * 32768
    }
    result = validate_record(record)
    assert result.is_valid == False
    assert result.rejection_reason == "tcp_payload_overflow"

