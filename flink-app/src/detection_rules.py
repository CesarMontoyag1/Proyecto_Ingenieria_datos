import collections
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

from src.pseudonymize import pseudonymize_mac

# Constantes de Subtipos 802.11 (Management Type = 0)
SUBTYPE_ASSOC_REQ = "0"
SUBTYPE_REASSOC_REQ = "2"
SUBTYPE_BEACON = "8"
SUBTYPE_DISAS = "10"
SUBTYPE_DEAUTH = "12"


def _clean_str(val: Any) -> str:
    """Normaliza cadenas eliminando decimales de flotantes (ej. '12.0' -> '12')."""
    if val is None:
        return ""
    s = str(val).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _parse_epoch(val: Any) -> float:
    """Extrae el timestamp en segundos desde frame.time_epoch o el tiempo actual."""
    if val is None:
        return time.time()
    try:
        return float(val)
    except (ValueError, TypeError):
        return time.time()


class DetectionEngine:
    """
    Motor de detección de intrusiones en streaming para tráfico Wi-Fi 802.11 (AWID3).
    Aplica reglas de estado con ventanas temporales y previene tormentas de alertas.
    Garantiza que toda dirección MAC en las alertas esté seudonimizada con HMAC-SHA256.
    """

    def __init__(
        self,
        hmac_key: bytes,
        deauth_threshold: int = 10,
        deauth_window_sec: float = 5.0,
        disas_threshold: int = 10,
        disas_window_sec: float = 5.0,
        ssdp_threshold: int = 15,
        ssdp_window_sec: float = 5.0,
        alert_cooldown_sec: float = 60.0,
        ssid_whitelist: Optional[Dict[str, Set[str]]] = None
    ):
        self.hmac_key = hmac_key
        self.deauth_threshold = deauth_threshold
        self.deauth_window_sec = deauth_window_sec
        self.disas_threshold = disas_threshold
        self.disas_window_sec = disas_window_sec
        self.ssdp_threshold = ssdp_threshold
        self.ssdp_window_sec = ssdp_window_sec
        self.alert_cooldown_sec = float(os.environ.get("ALERT_COOLDOWN_SEC", str(alert_cooldown_sec)))

        # Lista blanca de BSSIDs legítimos por SSID: { 'CorpNet': {'00:11:22:33:44:55', ...} }
        self.ssid_whitelist = ssid_whitelist or {
            "NexaSecure-Corp": {"00:14:6c:7e:40:80", "00:14:6c:7e:40:81"},
            "NexaSecure-Guest": {"00:14:6c:7e:40:90"}
        }

        # Ventanas deslizantes de eventos:
        # deauth: { bssid: deque([(timestamp, sa), ...]) }
        self.deauth_events: Dict[str, collections.deque] = collections.defaultdict(collections.deque)
        # disas/assoc: { sa: deque([(timestamp, bssid), ...]) }
        self.disas_events: Dict[str, collections.deque] = collections.defaultdict(collections.deque)
        # ssdp flood: { bssid: deque([(timestamp, sa), ...]) }
        self.ssdp_events: Dict[str, collections.deque] = collections.defaultdict(collections.deque)

        # Registro de deduplicación de alertas: { (rule_type, bssid): last_alert_time }
        self.last_alert_time: Dict[Tuple[str, str], float] = {}

    def load_rules_from_rds(self, db_conn) -> bool:
        """
        Carga umbrales y severidades dinámicamente desde la tabla rule_catalog en RDS.
        Permite recalibrar la sensibilidad de detección sin requerir redeploy del job de Flink.
        """
        if not db_conn:
            return False
        try:
            with db_conn.cursor() as cur:
                cur.execute(
                    "SELECT rule_type, default_severity, default_threshold, default_window_seconds "
                    "FROM rule_catalog WHERE enabled = TRUE"
                )
                rows = cur.fetchall()
                for rule_type, severity, threshold, window_sec in rows:
                    if rule_type == "DEAUTH_FLOOD":
                        self.deauth_threshold = int(threshold)
                        self.deauth_window_sec = float(window_sec)
                    elif rule_type == "DISAS_BURST":
                        self.disas_threshold = int(threshold)
                        self.disas_window_sec = float(window_sec)
                    elif rule_type == "SSDP_FLOOD":
                        self.ssdp_threshold = int(threshold)
                        self.ssdp_window_sec = float(window_sec)
            return True
        except Exception:
            return False

    def _should_suppress_alert(self, rule_type: str, bssid: str, current_time: float) -> bool:
        """Verifica si la alerta está en periodo de enfriamiento (cooldown)."""
        key = (rule_type, bssid)
        last_time = self.last_alert_time.get(key)
        if last_time is not None and (current_time - last_time) < self.alert_cooldown_sec:
            return True
        self.last_alert_time[key] = current_time
        return False

    def check_deauth_flood(self, record: Dict[str, Any], event_time: float) -> Optional[Dict[str, Any]]:
        """
        Regla 1: Deauth Flood
        Detecta ráfagas de más de N tramas de deautenticación hacia el mismo BSSID en 5 segundos.
        """
        fc_type = _clean_str(record.get("wlan.fc.type"))
        fc_subtype = _clean_str(record.get("wlan.fc.subtype"))

        if fc_type != "0" or fc_subtype != SUBTYPE_DEAUTH:
            return None

        raw_bssid = _clean_str(record.get("wlan.bssid") or record.get("wlan.da"))
        raw_sa = _clean_str(record.get("wlan.sa") or record.get("wlan.ta"))

        if not raw_bssid:
            return None

        # Limpiar eventos fuera de la ventana deslizante
        q = self.deauth_events[raw_bssid]
        q.append((event_time, raw_sa))
        min_time = event_time - self.deauth_window_sec
        while q and q[0][0] < min_time:
            q.popleft()

        count = len(q)
        if count >= self.deauth_threshold:
            pseudo_bssid = pseudonymize_mac(raw_bssid, self.hmac_key)
            if self._should_suppress_alert("DEAUTH_FLOOD", pseudo_bssid, event_time):
                return None

            pseudo_sa = pseudonymize_mac(raw_sa, self.hmac_key) if raw_sa else "N/A"
            return {
                "alert_id": str(uuid.uuid4()),
                "rule_type": "DEAUTH_FLOOD",
                "severity": "HIGH",
                "bssid": pseudo_bssid,
                "src_mac": pseudo_sa,
                "observed_count": count,
                "threshold": self.deauth_threshold,
                "window_seconds": self.deauth_window_sec,
                "timestamp": event_time,
                "details": {
                    "reason": "Ráfaga masiva de tramas de deautenticación detectada",
                    "reason_code": _clean_str(record.get("wlan.fixed.reason_code", "N/A"))
                }
            }
        return None

    def check_rogue_ap(self, record: Dict[str, Any], event_time: float) -> Optional[Dict[str, Any]]:
        """
        Regla 2: Rogue AP / Evil Twin
        Detecta un SSID corporativo anunciado por un BSSID que no está en la lista blanca legítima.
        """
        ssid = _clean_str(record.get("wlan.ssid"))
        raw_bssid = _clean_str(record.get("wlan.bssid") or record.get("wlan.ta"))

        if not ssid or not raw_bssid:
            return None

        if ssid in self.ssid_whitelist:
            allowed_bssids = self.ssid_whitelist[ssid]
            # Normalizar BSSID a minúsculas
            if raw_bssid.lower() not in {b.lower() for b in allowed_bssids}:
                pseudo_bssid = pseudonymize_mac(raw_bssid, self.hmac_key)
                if self._should_suppress_alert("ROGUE_AP", pseudo_bssid, event_time):
                    return None

                return {
                    "alert_id": str(uuid.uuid4()),
                    "rule_type": "ROGUE_AP",
                    "severity": "CRITICAL",
                    "bssid": pseudo_bssid,
                    "src_mac": pseudo_bssid,
                    "observed_count": 1,
                    "threshold": 1,
                    "window_seconds": 1.0,
                    "timestamp": event_time,
                    "details": {
                        "ssid": ssid,
                        "reason": f"SSID legítimo '{ssid}' transmitido desde BSSID no autorizado (Rogue AP/Evil Twin)"
                    }
                }
        return None

    def check_disas_burst(self, record: Dict[str, Any], event_time: float) -> Optional[Dict[str, Any]]:
        """
        Regla 3: Disas Burst / Anomaly
        Detecta ráfagas de disasociación o reasociación anómala por dispositivo origen en 10 segundos.
        """
        fc_type = _clean_str(record.get("wlan.fc.type"))
        fc_subtype = _clean_str(record.get("wlan.fc.subtype"))

        if fc_type != "0" or fc_subtype not in (SUBTYPE_DISAS, SUBTYPE_ASSOC_REQ, SUBTYPE_REASSOC_REQ):
            return None

        raw_bssid = _clean_str(record.get("wlan.bssid") or record.get("wlan.da"))
        raw_sa = _clean_str(record.get("wlan.sa") or record.get("wlan.ta"))

        if not raw_sa or not raw_bssid:
            return None

        q = self.disas_events[raw_sa]
        q.append((event_time, raw_bssid))
        min_time = event_time - self.disas_window_sec
        while q and q[0][0] < min_time:
            q.popleft()

        count = len(q)
        if count >= self.disas_threshold:
            pseudo_bssid = pseudonymize_mac(raw_bssid, self.hmac_key)
            if self._should_suppress_alert("DISAS_BURST", pseudo_bssid, event_time):
                return None

            pseudo_sa = pseudonymize_mac(raw_sa, self.hmac_key)
            return {
                "alert_id": str(uuid.uuid4()),
                "rule_type": "DISAS_BURST",
                "severity": "MEDIUM",
                "bssid": pseudo_bssid,
                "src_mac": pseudo_sa,
                "observed_count": count,
                "threshold": self.disas_threshold,
                "window_seconds": self.disas_window_sec,
                "timestamp": event_time,
                "details": {
                    "reason": f"Ráfaga de subtipos {fc_subtype} (disas/assoc) por dispositivo origen",
                    "subtype": fc_subtype
                }
            }
        return None

    def check_ssdp_flood(self, record: Dict[str, Any], event_time: float) -> Optional[Dict[str, Any]]:
        """
        Regla 4: SSDP Flood / Amplification DoS
        Detecta ráfagas masivas de paquetes SSDP/UPnP (puerto UDP 1900 o campo ssdp activo en AWID3)
        hacia un mismo BSSID/dispositivo en una ventana corta de tiempo.
        """
        is_ssdp = False
        ssdp_val = _clean_str(record.get("ssdp"))
        if ssdp_val and ssdp_val.lower() not in ("0", "false", "nan", "none", ""):
            is_ssdp = True
        elif _clean_str(record.get("udp.dstport")) == "1900" or _clean_str(record.get("udp.srcport")) == "1900":
            is_ssdp = True
        elif _clean_str(record.get("Label")).lower() == "ssdp":
            is_ssdp = True

        if not is_ssdp:
            return None

        raw_bssid = _clean_str(record.get("wlan.bssid") or record.get("wlan.da") or record.get("wlan.ra"))
        raw_sa = _clean_str(record.get("wlan.sa") or record.get("wlan.ta") or record.get("ip.src"))

        if not raw_bssid:
            raw_bssid = "00:00:00:00:00:00"

        q = self.ssdp_events[raw_bssid]
        q.append((event_time, raw_sa))
        min_time = event_time - self.ssdp_window_sec
        while q and q[0][0] < min_time:
            q.popleft()

        count = len(q)
        if count >= self.ssdp_threshold:
            pseudo_bssid = pseudonymize_mac(raw_bssid, self.hmac_key)
            if self._should_suppress_alert("SSDP_FLOOD", pseudo_bssid, event_time):
                return None

            pseudo_sa = pseudonymize_mac(raw_sa, self.hmac_key) if raw_sa else "N/A"
            return {
                "alert_id": str(uuid.uuid4()),
                "rule_type": "SSDP_FLOOD",
                "severity": "HIGH",
                "bssid": pseudo_bssid,
                "src_mac": pseudo_sa,
                "observed_count": count,
                "threshold": self.ssdp_threshold,
                "window_seconds": self.ssdp_window_sec,
                "timestamp": event_time,
                "details": {
                    "reason": "Ráfaga masiva de paquetes SSDP/UPnP detectada (DDoS de amplificación / saturación Wi-Fi)",
                    "protocol": "SSDP/UDP 1900"
                }
            }
        return None

    def process_event(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Evalúa todas las reglas de detección sobre el registro y retorna una lista de alertas generadas.
        """
        event_time = _parse_epoch(record.get("frame.time_epoch"))
        alerts = []

        # Regla 1: Deauth Flood
        a1 = self.check_deauth_flood(record, event_time)
        if a1:
            alerts.append(a1)

        # Regla 2: Rogue AP
        a2 = self.check_rogue_ap(record, event_time)
        if a2:
            alerts.append(a2)

        # Regla 3: Disas Burst
        a3 = self.check_disas_burst(record, event_time)
        if a3:
            alerts.append(a3)

        # Regla 4: SSDP Flood
        a4 = self.check_ssdp_flood(record, event_time)
        if a4:
            alerts.append(a4)

        return alerts

