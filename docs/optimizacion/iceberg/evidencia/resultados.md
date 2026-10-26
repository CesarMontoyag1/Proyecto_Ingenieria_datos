# Athena: optimización de Trusted

Mismas filas y filtros; reutilización de resultados desactivada. Medianas de ejecuciones; los cambios se miden como conjunto, sin atribuir por separado beneficios a bloom filters.

| Consulta | Antes ms | Después ms | Antes bytes | Después bytes | Reducción bytes |
|---|---:|---:|---:|---:|---:|
| date_label | 1040 | 673 | 748348 | 54304 | 92.74% |
| bssid | 771 | 1474 | 238436 | 127250 | 46.63% |

Los JSON contienen SQL, identificadores de consulta, snapshots y resultados para reproducir/verificar el comparativo.

Una mejora de tiempo no está garantizada en un dataset pequeño. La equivalencia de resultados es obligatoria.
