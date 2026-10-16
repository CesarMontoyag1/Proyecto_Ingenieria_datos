import pytest
from pyspark.sql.functions import col, struct, udf
from pyspark.sql.types import StringType
from src.schema_validation import validate_record_logic
from src.pseudonymize import pseudonymize_mac_logic, get_pseudonymize_udf

def test_validate_record_logic_valid():
    record = {
        "frame.time_epoch": "1608227266.862492000",
        "frame.len": "128",
        "wlan.fc.type": "0",
        "wlan.bssid": "00:11:22:33:44:55",
        "wlan.sa": "00:11:22:33:44:55",
        "wlan.da": "ff:ff:ff:ff:ff:ff",
        "Label": "Normal"
    }
    result = validate_record_logic(record)
    assert result == ""

def test_validate_record_logic_control_frame_valid():
    # ACK / CTS / RTS frame (type 1) has no bssid or sa, only receiver address (da/ra)
    record = {
        "frame.time_epoch": "1608227266.862492000",
        "frame.len": "14",
        "wlan.fc.type": "1",
        "wlan.da": "00:11:22:33:44:55",
        "Label": "Normal"
    }
    result = validate_record_logic(record)
    assert result == ""

def test_validate_record_logic_control_frame_missing_receiver():
    record = {
        "frame.time_epoch": "1608227266.862492000",
        "frame.len": "14",
        "wlan.fc.type": "1",
        "Label": "Normal"
    }
    result = validate_record_logic(record)
    assert "missing_receiver_address_in_control_frame" in result

def test_validate_record_logic_data_missing_bssid():
    record = {
        "frame.time_epoch": "1608227266.862492000",
        "frame.len": "128",
        "wlan.fc.type": "2",
        "wlan.sa": "00:11:22:33:44:55",
        "Label": "Normal"
    }
    result = validate_record_logic(record)
    assert "missing_bssid_in_data_or_mgmt_frame" in result

def test_validate_record_logic_tcp_overflow():
    record = {
        "frame.time_epoch": "1608227266.862492000",
        "frame.len": "35000",
        "wlan.fc.type": "2",
        "wlan.bssid": "00:11:22:33:44:55",
        "wlan.sa": "00:11:22:33:44:55",
        "tcp.payload": "A" * 33000,
        "Label": "Normal"
    }
    result = validate_record_logic(record)
    assert "tcp_payload_overflow" in result

def test_validate_record_logic_missing_critical():
    record = {
        "frame.len": "128",
        "Label": "Normal"
    }
    result = validate_record_logic(record)
    assert "missing_critical_column" in result

def test_validate_with_spark(spark):
    data = [
        ("1608227266", "128", "00:11:22:33:44:55", "00:11:22:33:44:55", "ff:ff:ff:ff:ff:ff", "Normal"),
        (None, "128", "00:11:22:33:44:55", "00:11:22:33:44:55", "ff:ff:ff:ff:ff:ff", "Normal")
    ]
    df = spark.createDataFrame(data, ["frame.time_epoch", "frame.len", "wlan.bssid", "wlan.sa", "wlan.da", "Label"])
    
    def row_to_dict_validation(row):
        return validate_record_logic(row.asDict())
        
    validate_udf = udf(row_to_dict_validation, StringType())
    struct_col = struct([df[x] for x in df.columns])
    
    df_result = df.withColumn("rejection", validate_udf(struct_col))
    results = df_result.collect()
    
    assert results[0].rejection == ""
    assert "missing_critical_column" in results[1].rejection

def test_pseudonymize_mac_logic():
    key = b"secret"
    mac = "00:11:22:33:44:55"
    pseudo = pseudonymize_mac_logic(mac, key)
    assert pseudo.startswith("00:11:22:")
    assert len(pseudo) > 9
    assert pseudo != mac

