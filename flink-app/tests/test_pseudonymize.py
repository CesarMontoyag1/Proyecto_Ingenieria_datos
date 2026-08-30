import pytest
from src.pseudonymize import pseudonymize_mac

def test_pseudonymize_mac():
    key = b"supersecretkey"
    mac = "00:11:22:33:44:55"
    result = pseudonymize_mac(mac, key)
    
    assert result.startswith("00:11:22:")
    assert len(result) > 9
    assert result != mac

def test_pseudonymize_invalid_mac():
    key = b"supersecretkey"
    mac = "invalid-mac"
    result = pseudonymize_mac(mac, key)
    
    assert result != mac
    assert ":" not in result

