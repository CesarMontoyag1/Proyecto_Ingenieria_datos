# Monitoreo y alertas operativas

Stack `nexasecure-monitoring-{stage}`. Vigila la salud del pipeline y avisa por correo
cuando algo falla. Es independiente del tópico del SOC (`nexasecure-soc-alerts-{stage}`):
este tópico avisa que **el pipeline falló**; el del SOC avisa que **hay un ataque**.

## Qué crea

| Recurso | Detalle |
|---|---|
| Tópico SNS `nexasecure-ops-alerts-{stage}` | Suscripción por correo para cada dirección configurada. Su política solo permite publicar a CloudWatch, a las reglas `nexasecure-*` de EventBridge y a AWS Budgets de esta cuenta |
| 8 alarmas de CloudWatch | DLQ con mensajes, cola de alertas atascada, errores y *throttling* de la Lambda, atraso de los consumidores de Kinesis, escrituras rechazadas por Kinesis, CPU y almacenamiento de RDS |
| 3 alarmas opcionales | Entrega de Firehose (con `FIREHOSE_STREAM_NAME`), SLA de latencia y detector detenido (con `ENABLE_DETECTOR_ALARMS=true`) |
| 4 reglas de EventBridge | Job de Glue fallido, crawler fallido, paso del detector fallido en EMR y clúster EMR terminado con errores |
| Presupuesto mensual | Avisos al 50 % y al 80 % del gasto real y al 100 % del pronóstico. Es de toda la cuenta, no solo de NexaSecure |
| Dashboard `nexasecure-wids-{stage}` | Estado de las alarmas, Kinesis, detector, SQS, Lambda y RDS |
| Parámetros SSM | `/nexasecure/{stage}/monitoring/OpsAlertsTopicArn` y `/nexasecure/{stage}/monitoring/DashboardName` |

Todas las alarmas notifican al entrar en `ALARM` y al volver a `OK`.

## Despliegue

Requisitos: los stacks `queue`, `notifications`, `streaming`, `rds` y la Lambda de alertas
ya desplegados, porque las alarmas usan sus nombres.

Desde el contenedor de `infrastructure`:

```bash
cd /app/infrastructure/serverless/monitoring
serverless deploy --stage dev
```

Con la configuración actual no hace falta exportar nada: `GIT_USER_NAME` toma el valor
`local` por defecto y el correo se lee de `ALERT_EMAIL` en `.envs/sls.env`. Las variables
de la tabla solo se usan para cambiar ese comportamiento.

Variables:

| Variable | Obligatoria | Uso |
|---|---|---|
| `GIT_USER_NAME` | No | Sufijo de los nombres de recursos; por defecto `local`, que es el valor de esta cuenta. Solo cambia si los stacks se desplegaron con otro sufijo |
| `OPS_ALERT_EMAILS` | No | Correos separados por coma, para enviar los avisos de operación a otras direcciones. Si no existe, usa `ALERT_EMAILS` o `ALERT_EMAIL`, y después esas mismas claves en `.envs/sls.env` |
| `MONTHLY_BUDGET_USD` | No | Límite del presupuesto mensual en dólares |
| `FIREHOSE_STREAM_NAME` | No | Nombre del delivery stream de Firehose. Lo genera CloudFormation; se ve en la consola de Firehose (empieza por `nexasecure-streaming-dev-Awid3RawDeliveryStream-`) |
| `ENABLE_DETECTOR_ALARMS` | No | `true` cuando el detector publique sus métricas propias (ver abajo) |

Después del despliegue, **cada destinatario debe confirmar la suscripción** desde el correo
"AWS Notification - Subscription Confirmation". Sin esa confirmación no llega ningún aviso.

## Métricas propias del detector

El dashboard y las alarmas opcionales esperan que `flink-app/src/detection_job.py` publique en:

- Namespace: `NexaSecure/WIDS`
- Dimensión: `Stage` = `{stage}`
- Métricas: `EventsProcessed`, `EventsQuarantined`, `AlertsGenerated` (conteos) y
  `DetectionLatencyMs` (milisegundos desde la llegada a Kinesis hasta la publicación en SQS)

El detector publica estas métricas cada 60 s (`METRICS_FLUSH_SECONDS`), incluso con
conteos en 0, desde la versión que mide la latencia con `ApproximateArrivalTimestamp`.
Mientras EMR ejecute una versión anterior, esos dos gráficos aparecen vacíos y las
alarmas opcionales deben quedar desactivadas. Con la versión nueva corriendo, se activan así:

```bash
ENABLE_DETECTOR_ALARMS=true serverless deploy --stage dev
```

## Cómo probarlo (sirve como demo)

**1. Prueba rápida de notificación.** Fuerza el estado de una alarma; CloudWatch la
devuelve a `OK` en la siguiente evaluación.

```bash
aws cloudwatch set-alarm-state \
  --alarm-name nexasecure-dev-alerts-dlq-not-empty \
  --state-value ALARM --state-reason "Prueba manual del monitoreo"
```

**2. Prueba real de punta a punta.** La Lambda no puede procesar un mensaje que no es
JSON y lo devuelve a la cola (`batchItemFailures`). Tras 3 intentos (unos 3 minutos con el
*visibility timeout* de 60 s) el mensaje pasa a la DLQ, se activa la alarma
`alerts-dlq-not-empty` y llega el correo. La alarma de errores de la Lambda no se activa
en esta prueba, porque la función termina bien y solo reporta el mensaje fallido.

```bash
QUEUE_URL=$(aws ssm get-parameter --name /nexasecure/dev/queue/AlertsQueueUrl \
  --query Parameter.Value --output text)
aws sqs send-message --queue-url "$QUEUE_URL" --message-body "esto-no-es-json"
```

Para limpiar la DLQ después de la prueba:

```bash
DLQ_URL=$(aws ssm get-parameter --name /nexasecure/dev/queue/AlertsDLQUrl \
  --query Parameter.Value --output text)
aws sqs purge-queue --queue-url "$DLQ_URL"
```

**3. Fallo de Glue (opcional, consume unos minutos de Glue).** Un job con una base de
datos inexistente falla antes de escribir datos y no avanza el *bookmark*.

```bash
aws glue start-job-run --job-name nexasecure_batch_ingest_dev_local \
  --arguments '{"--CATALOG_DATABASE_NAME":"base_que_no_existe"}'
```

El dashboard está en CloudWatch → Dashboards → `nexasecure-wids-dev`.
