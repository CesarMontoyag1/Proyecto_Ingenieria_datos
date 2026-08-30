import hmac
import hashlib
import os

def pseudonymize_mac(mac_address: str, secret_key: str = None) -> str:
    """
    Seudonimiza una dirección MAC (o BSSID) usando HMAC-SHA256.
    La llave se debe inyectar vía Secrets Manager.
    """
    if not secret_key:
        secret_key = os.environ.get('HMAC_SECRET_KEY', 'default-dev-key')
        
    h = hmac.new(secret_key.encode('utf-8'), mac_address.encode('utf-8'), hashlib.sha256)
    return h.hexdigest()

