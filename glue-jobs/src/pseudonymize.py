import hmac
import hashlib

try:
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
except ImportError:
    udf = None
    StringType = None

BROADCAST_MAC = "ff:ff:ff:ff:ff:ff"

def pseudonymize_mac_logic(mac: str, hmac_key: bytes) -> str:
    """
    Applies HMAC-SHA256 to a MAC address while preserving the first 3 octets (OUI prefix).
    """
    if not mac:
        return mac

    if mac.lower() == BROADCAST_MAC:
        return mac

    if len(mac) < 8:
        return mac

    parts = mac.split(':')
    if len(parts) != 6:
        h = hmac.new(hmac_key, mac.encode('utf-8'), hashlib.sha256)
        return h.hexdigest()

    oui_prefix = ":".join(parts[:3])
    device_suffix = ":".join(parts[3:])
    
    h = hmac.new(hmac_key, device_suffix.encode('utf-8'), hashlib.sha256)
    hashed_suffix = h.hexdigest()[:8]

    return f"{oui_prefix}:{hashed_suffix}"

def get_pseudonymize_udf(hmac_key_str: str):
    """
    Returns a PySpark UDF with the HMAC key bound via closure.
    """
    if udf is None:
        raise RuntimeError("PySpark no está disponible en este entorno.")
    key_bytes = hmac_key_str.encode('utf-8')
    return udf(lambda mac: pseudonymize_mac_logic(mac, key_bytes), StringType())


