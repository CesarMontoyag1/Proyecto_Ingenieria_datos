import hmac
import hashlib

def pseudonymize_mac(mac: str, hmac_key: bytes) -> str:
    """
    Applies HMAC-SHA256 to a MAC address while preserving the first 3 octets (OUI prefix).
    Example: 00:11:22:33:44:55 -> 00:11:22:<hash>
    """
    if not mac or len(mac) < 8:
        return mac

    parts = mac.split(':')
    if len(parts) != 6:
        # Not a valid MAC format, hash it completely
        h = hmac.new(hmac_key, mac.encode('utf-8'), hashlib.sha256)
        return h.hexdigest()

    oui_prefix = ":".join(parts[:3])
    device_suffix = ":".join(parts[3:])
    
    h = hmac.new(hmac_key, device_suffix.encode('utf-8'), hashlib.sha256)
    hashed_suffix = h.hexdigest()[:8] # taking first 8 chars for brevity and valid-looking length

    return f"{oui_prefix}:{hashed_suffix}"

