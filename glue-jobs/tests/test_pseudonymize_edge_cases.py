"""Casos borde de pseudonymize_mac_logic que test_schema_validation.py no
cubre todavía. Colocar junto a glue-jobs/tests/test_schema_validation.py
(o fusionar con ese archivo) -- usa el mismo import que ya existe ahí:

    from src.pseudonymize import pseudonymize_mac_logic

El primer test (broadcast) está escrito para FALLAR con el código actual:
es la tarea de Luis. pseudonymize_mac_logic en glue-jobs/src/pseudonymize.py
hashea CUALQUIER MAC de 6 octetos, incluida ff:ff:ff:ff:ff:ff, tal como lo
documentó Sebastián en docs/gobernanza/estado_gobernanza.md ("2.504
registros tienen la dirección de broadcast seudonimizada... conviene
excluir broadcast y multicast de la función").

Corrección esperada (una condición, al inicio de pseudonymize_mac_logic):
    BROADCAST_MAC = "ff:ff:ff:ff:ff:ff"
    if mac.lower() == BROADCAST_MAC:
        return mac  # no pseudonimizar: no identifica a nadie

Multicast (primer octeto con el bit menos significativo en 1, p. ej.
01:00:5e:...) es el mismo principio pero opcional para esta entrega --
dejarlo como comentario/TODO si no alcanza el tiempo.
"""
from src.pseudonymize import pseudonymize_mac_logic


def test_broadcast_mac_is_not_pseudonymized():
    # Hoy FALLA: el código actual no distingue broadcast de una MAC real.
    key = b"secret"
    broadcast = "ff:ff:ff:ff:ff:ff"
    result = pseudonymize_mac_logic(broadcast, key)
    assert result == broadcast, (
        "La dirección de broadcast no debe pseudonimizarse: no identifica "
        "ningún dispositivo y ocultarla impide distinguir tráfico dirigido "
        "de difundido, señal relevante para detectar deauth floods."
    )


def test_non_standard_mac_format_is_fully_hashed():
    # Ya pasa hoy -- cubre la rama "len(parts) != 6" que no tenía test.
    key = b"secret"
    malformed = "not-a-real-mac"
    result = pseudonymize_mac_logic(malformed, key)
    assert result != malformed
    assert ":" not in result  # se hashea completo, sin preservar OUI


def test_same_mac_same_key_is_deterministic():
    # Propiedad que el analista SOC necesita: la misma MAC real debe
    # producir siempre el mismo pseudónimo para poder correlacionar
    # apariciones repetidas del mismo dispositivo entre corridas del job.
    key = b"secret"
    mac = "aa:bb:cc:11:22:33"
    assert pseudonymize_mac_logic(mac, key) == pseudonymize_mac_logic(mac, key)


def test_different_keys_produce_different_pseudonyms():
    # Si la llave HMAC rota (ver Secrets Manager), el pseudónimo debe
    # cambiar -- si no, la rotación de llaves no serviría de nada.
    mac = "aa:bb:cc:11:22:33"
    assert pseudonymize_mac_logic(mac, b"key-one") != pseudonymize_mac_logic(mac, b"key-two")
