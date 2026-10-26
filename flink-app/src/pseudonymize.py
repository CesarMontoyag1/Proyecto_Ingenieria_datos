import hashlib
import hmac

BROADCAST_MAC = "ff:ff:ff:ff:ff:ff"

def pseudonymize_mac(mac: str, hmac_key: bytes) -> str:
    """
    Aplica HMAC-SHA256 a una dirección MAC conservando los primeros 3 octetos (prefijo OUI).
    Excluye explícitamente la dirección broadcast (ff:ff:ff:ff:ff:ff) para no impedir
    la detección de ataques como Deauth broadcast.
    """
    if not mac:
        return mac

    if mac.lower() == BROADCAST_MAC:
        return mac

    if len(mac) < 8:
        return mac

    clean_mac = mac.strip().lower()
    if clean_mac == "broadcast":
        return clean_mac

    parts = clean_mac.split(":")
    if len(parts) != 6:
        # Formato no estándar, hashear completo
        h = hmac.new(hmac_key, clean_mac.encode("utf-8"), hashlib.sha256)
        return h.hexdigest()

    oui_prefix = ":".join(parts[:3])
    device_suffix = ":".join(parts[3:])

    h = hmac.new(hmac_key, device_suffix.encode("utf-8"), hashlib.sha256)
    hashed_suffix = h.hexdigest()[:8]

    return f"{oui_prefix}:{hashed_suffix}"

pseudonymize_mac_logic = pseudonymize_mac
