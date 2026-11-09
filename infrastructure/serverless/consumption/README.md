# Capa de consumo: zona Serving, Athena y QuickSight

```
Detector (EMR) → SQS → Lambda alertFormatter ─┬→ SNS (correo al SOC)
                                               └→ S3 trusted/serving/alerts/dt=YYYY-MM-DD/*.json
Glue Job batch → trusted_awid3 (Iceberg)
                       │                                  │
                       └────────── Athena (workgroup nexasecure-consumption-dev) ──→ vistas vw_* ──→ QuickSight (SPICE)
```

El dashboard solo lee datos gobernados: `trusted_awid3` (seudonimizada) y `alerts_serving`.
Nunca lee Raw ni Quarantine, que contienen MAC en texto plano.

## Qué hay en el repositorio

| Archivo | Contenido |
|---|---|
| `backend/src/alert_formatter/handler.py` | La Lambda guarda cada alerta en `s3://<trusted>/serving/alerts/dt=YYYY-MM-DD/<alert_id>.json` antes de enviar el correo. Si S3 falla, el mensaje vuelve a la cola sin enviar el correo; el reintento sobrescribe el mismo objeto, así que no hay duplicados |
| `backend/serverless/alerts/serverless.yml` | Variables `SERVING_BUCKET`/`SERVING_PREFIX` y permiso `s3:PutObject` solo sobre `serving/alerts/*` |
| `glue-tables.yml` | Tabla `alerts_serving` (JSON) con *partition projection* por `dt`: no necesita crawler |
| `athena.yml` | Workgroup `nexasecure-consumption-{stage}`: resultados en `s3://<trusted>/athena-results/consumption/`, métricas en CloudWatch y corte de 1 GiB por consulta |
| `sql/serving_views.sql` | Vistas `vw_alerts`, `vw_alerts_hourly`, `vw_traffic_daily` y `vw_top_bssid` |
| `scripts/athena_views.py` | Crea o reemplaza las vistas en Athena |

Vistas:

| Vista | Fuente | Para qué gráfica |
|---|---|---|
| `vw_alerts` | `alerts_serving` | Tabla de últimas alertas, KPIs, cumplimiento del SLA (`sla_status`) |
| `vw_alerts_hourly` | `vw_alerts` | Línea de tiempo por regla y severidad, latencia p50/p95 por hora |
| `vw_traffic_daily` | `trusted_awid3` | Tráfico por día, etiqueta AWID3, clase (Normal/Ataque) y tipo de trama |
| `vw_top_bssid` | `trusted_awid3` | Puntos de acceso más atacados. Excluye broadcast y los BSSID con menos de 5 tramas (k ≥ 5) |

## Despliegue (una vez, en este orden)

Desde el contenedor de `infrastructure`:

```bash
# 1. Lambda con permisos y variables de la zona Serving (empaqueta backend/serverless/alerts/handler.py)
cd /app/backend/serverless/alerts
serverless deploy --stage dev

# 2. Tabla alerts_serving y workgroup de Athena
cd /app/infrastructure/serverless/consumption
serverless deploy --stage dev

# 3. Vistas (el script lee .envs/aws.env)
cd /app
pip install -q boto3        # si el contenedor no lo tiene
python3 scripts/athena_views.py --stage dev --check
```

El paso 3 debe imprimir `OK` para las 4 vistas y las filas de muestra. `vw_alerts` y
`vw_alerts_hourly` salen vacías hasta que haya alertas nuevas (paso siguiente).

### Generar alertas para el dashboard

Con el detector corriendo en EMR, envía el ataque simulado o el simulador de Kaggle
(ver `infrastructure/serverless/monitoring/README.md`). Para comprobar que se guardan:

```bash
TRUSTED=$(aws ssm get-parameter --name /nexasecure/dev/storage/TrustedAwid3BucketName --query Parameter.Value --output text)
aws s3 ls "s3://$TRUSTED/serving/alerts/" --recursive | tail -5
```

En Athena (workgroup `nexasecure-consumption-dev`):

```sql
SELECT rule_type, severity, COUNT(*) AS alerts, approx_percentile(detection_latency_ms, 0.95) AS p95_ms
FROM vw_alerts GROUP BY 1, 2 ORDER BY alerts DESC;
```

## Dashboard de QuickSight

El dashboard se crea como código con el stack [infrastructure/serverless/quicksight](../quicksight/README.md):
rol de QuickSight, fuente de datos Athena, los 4 datasets en SPICE con refresco programado
y el dashboard `NexaSecure WIDS - SOC`. Lo único manual es suscribir la cuenta a QuickSight
una vez.

## Fuera de alcance por ahora

- Redshift Serverless con Spectrum como segunda fuente de QuickSight (pendiente).
- Los micro-lotes NDJSON que el detector escribe en `trusted/streaming/` no están
  catalogados; el tráfico del dashboard viene de la ruta batch (`trusted_awid3`).
