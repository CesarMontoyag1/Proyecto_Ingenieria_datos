# Gobernanza y calidad de datos

Datos de la corrida `jr_e71d4c3d12bd1f56` del 30 de agosto, 00:17. Archivo de entrada: `Deauth_0.csv`, 50.000 filas.

## Perfilamiento

De las 50.000 filas ingeridas, 14.066 pasaron los controles de calidad y 35.934 fueron rechazadas. La tasa de rechazo es del 71,87%.

Tres de las seis columnas críticas están completas al 100% (`frame.time_epoch`, `frame.len` y `Label`). Las otras tres, todas direcciones MAC, están apenas al 28%: `wlan.bssid` 28,15%, `wlan.sa` 28,15%, `wlan.da` 28,14%. La meta del Sprint 0 era superar el 98%, así que no se cumple.

Sobre la distribución de clases hay un problema: el 100% de los registros están etiquetados como `Normal`. Aunque el archivo cargado pertenece a la familia Deauth del dataset AWID3, el fragmento que se ingirió no contiene un solo ataque.

## Cuarentena

Los 35.934 registros rechazados se reparten así:

| Motivo | Registros |
|---|---|
| `missing_critical_column:wlan.bssid` | 35.925 |
| `missing_critical_column:wlan.da` | 5 |
| `missing_critical_column:wlan.sa` | 4 |

Prácticamente todo el rechazo viene de una sola regla. Ni el control de desbordamiento de `tcp.payload` ni el de errores TLS produjeron un rechazo, y al revisarlo contra los datos crudos no había candidatos para ninguno de los dos: el payload más largo mide 5.793 caracteres frente a un umbral de 32.767, y no hay ocurrencias de `bad_record_mac`. La columna `tls.decryption_failed`, que la regla también consulta, no existe en el esquema de 254 columnas del dataset. Dos de las tres reglas de calidad, entonces, nunca se han ejercitado.

Vale la pena anotar que 14.066 + 35.934 da exactamente 50.000, lo que confirma que ninguna fila se perdió entre zonas.

## Por qué se rechaza el 72%

Cruzando las filas rechazadas con el tipo de trama (`wlan.fc.type`) aparece la explicación: 35.922 de las 35.925 son tramas de control.

| | Control (1) | Datos (2) | Gestión (0) |
|---|---|---|---|
| Rechazadas | 35.922 | 3 | 0 |
| Aceptadas | 0 | 11.622 | 2.453 |

Las tramas de control del estándar 802.11 (ACK, RTS, CTS) no llevan campo BSSID. Su ausencia no es un defecto de los datos sino el comportamiento normal del protocolo, y la regla está exigiendo un campo que el estándar no define para ese tipo de trama.

El costo no es solo de volumen. Para un sistema de detección de intrusiones, las tramas de control son donde se manifiestan los ataques de RTS/CTS flood y ACK spoofing, de modo que el pipeline está mandando a cuarentena la evidencia de una familia entera de ataques. La corrección pasa por condicionar la criticidad de `wlan.bssid` al tipo de trama, o reemplazarlo por `wlan.ra` y `wlan.ta`, que sí están presentes en tramas de control.

## Auditoría de seudonimización

El control de privacidad funciona. Sobre los 14.066 registros de Trusted no queda ninguna dirección MAC en texto plano, verificado con una expresión regular que busca el patrón completo de seis octetos. 14.048 registros (99,87%) tienen el formato esperado, con el prefijo OUI legible y el identificador de dispositivo reemplazado por un hash HMAC-SHA256:

```
wlan.sa             wlan.da             wlan.bssid
0c:9d:92:ad2596f1   ff:ff:ff:65005248   0c:9d:92:ad2596f1
74:d0:2b:d906e028   ff:ff:ff:65005248   0c:9d:92:ad2596f1
```

Se conservan 34 fabricantes distintos, así que el dato sigue sirviendo para análisis por dispositivo. Los 18 registros restantes (0,13%) perdieron el OUI porque su MAC no venía con seis octetos y el código las hashea completas.

Hay un detalle a corregir: 2.504 registros tienen la dirección de broadcast seudonimizada. `ff:ff:ff:ff:ff:ff` no identifica a nadie, y hashearla impide distinguir tráfico dirigido de tráfico difundido, que es señal relevante para detectar deauth. Conviene excluir broadcast y multicast de la función.

## Estado de los controles

> Este es el párrafo que va al resumen ejecutivo.

Operan hoy dos controles automáticos dentro del job de ingesta. El primero valida seis campos obligatorios por fila y deriva lo que falla a la zona de cuarentena con el motivo registrado en `rejection_reason`, lo que permite auditar la calidad sin perder trazabilidad. El segundo seudonimiza las tres direcciones MAC con HMAC-SHA256 bajo una llave gestionada en Secrets Manager y cifrada con KMS, conservando el prefijo del fabricante y ofuscando el identificador de dispositivo. La capa Raw se escribe sin filtros previos, lo que garantiza inmutabilidad y permite reprocesar. La auditoría de la primera corrida confirma que el control de privacidad cumple, con cero direcciones MAC en texto plano sobre 14.066 registros, pero que el de calidad está mal calibrado: rechaza el 71,87% del dataset por exigir `wlan.bssid` en tramas de control 802.11, donde el estándar no define ese campo. Quedan pendientes para el Sprint 2 el catálogo de metadatos con OpenMetadata, el linaje con OpenLineage y un modelo RBAC por zona del Lakehouse, que hoy no existe: no hay separación entre quien opera el pipeline y quien lo audita.

## Pendientes menores

La zona de cuarentena se escribe como Parquet crudo, sin registro en el Glue Catalog, por lo que no era consultable desde Athena. Para este análisis se registró la tabla externa `quarantine_awid3`; en Sprint 2 debería migrarse a Iceberg con crawler propio.

También conviene homogeneizar la escritura entre zonas. Raw y Trusted usan `createOrReplace` y Quarantine usa `append`, de modo que si el job corre más de una vez las zonas se desincronizan y la tasa de rechazo queda inflada. Esta vez no ocurrió porque hubo una sola corrida exitosa, pero el riesgo está.
