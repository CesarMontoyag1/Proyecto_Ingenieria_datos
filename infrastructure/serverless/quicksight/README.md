# Dashboard de QuickSight como código

Stack `nexasecure-quicksight-{stage}`, definido en `quicksight.js`. Crea:

| Recurso | Detalle |
|---|---|
| Rol `nexasecure-quicksight-athena-{stage}` | QuickSight lo asume para consultar Athena. Solo puede usar el workgroup `nexasecure-consumption-{stage}`, leer el catálogo de NexaSecure y leer el bucket **Trusted**. No tiene acceso a Raw ni Quarantine |
| Fuente de datos Athena | `NexaSecure - Athena (Serving)` |
| 2 datasets de alertas **en vivo** (consulta directa a Athena) | `NexaSecure - Alertas (en vivo)` (`vw_alerts`) y `- Alertas por hora (en vivo)` (`vw_alerts_hourly`). Las alertas nuevas aparecen al abrir o actualizar el dashboard, sin refrescos ni pasos manuales |
| 2 datasets de tráfico en SPICE | `NexaSecure - Tráfico diario` (`vw_traffic_daily`) y `- Top BSSID` (`vw_top_bssid`), con refresco programado diario a las 06:00 (America/Bogota). Solo cambian con la ingesta batch |
| Dashboard `NexaSecure WIDS - SOC` | 6 KPIs, alertas por hora y regla, alertas por severidad, tramas por categoría AWID3, tráfico diario normal/ataque, tipo de trama 802.11, top de puntos de acceso atacados y últimas alertas |
| Parámetro SSM | `/nexasecure/{stage}/consumption/DashboardUrl` |

Para cambiar el dashboard se edita `quicksight.js` y se vuelve a desplegar; CloudFormation
publica una versión nueva. No se edita a mano en la consola.

## Requisitos

1. Stacks `consumption` desplegado y vistas creadas (`scripts/athena_views.py`).
2. Cuenta suscrita a QuickSight (una sola vez por cuenta, ver abajo).

## Paso 1. Suscribirse a QuickSight (una vez)

Es configuración de la cuenta, no del proyecto. CloudFormation no la puede crear.

1. Consola de AWS → buscar **QuickSight** → **Sign up**.
2. Región **US East (N. Virginia)**. Elegir un usuario **Author** estándar.
   **No elegir Author Pro ni Reader Pro, y no activar Amazon Q ni las funciones de IA**:
   agregan un cargo de 250 USD/mes por cuenta. Precios de referencia: Author 24 USD/mes
   (https://aws.amazon.com/quicksight/pricing/).
3. En el acceso a servicios de AWS **no hace falta marcar nada**: el stack crea su propio rol.
4. Terminar el registro.

## Paso 2. Guardar el ARN de tu usuario

En el contenedor de `infrastructure`:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
aws quicksight list-users --aws-account-id "$ACCOUNT" --namespace default \
  --query "UserList[].[UserName,Role,Arn]" --output table
```

Copia el ARN de tu usuario (rol `ADMIN` o `AUTHOR`) y agrégalo como una línea nueva en
`.envs/sls.env`:

```
QUICKSIGHT_USER_ARN=arn:aws:quicksight:us-east-1:609789571098:user/default/<tu-usuario>
```

## Paso 3. Desplegar

```bash
cd /app/infrastructure/serverless/quicksight
serverless deploy --stage dev
```

Las alertas no necesitan ningún refresco: sus datasets consultan Athena en vivo. Basta con
abrir o actualizar el dashboard (botón de actualizar del navegador) después de un ataque.

Los datasets de tráfico (SPICE) se cargan al crearse y se refrescan solos cada día. Solo si
se ejecuta una ingesta batch y se quiere ver el resultado de inmediato:

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
for DS in nexasecure-traffic-dev nexasecure-topbssid-dev; do
  aws quicksight create-ingestion --aws-account-id "$ACCOUNT" --data-set-id "$DS" \
    --ingestion-id "manual-$(date +%s)-$RANDOM" --query "[IngestionId,IngestionStatus]" --output text
done
```

## Paso 4. Abrir el dashboard

```bash
aws ssm get-parameter --name /nexasecure/dev/consumption/DashboardUrl --query Parameter.Value --output text
```

O en QuickSight → **Dashboards** → `NexaSecure WIDS - SOC`.

## Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `Define QUICKSIGHT_USER_ARN` al desplegar | Falta el paso 2 | Agregar la línea en `.envs/sls.env` |
| `ResourceNotFoundException` / cuenta no suscrita | Falta el paso 1 | Suscribirse a QuickSight |
| Una carga termina en `FAILED` | Permiso o tipo de columna | Ver `ErrorInfo.Message` con `list-ingestions` y reportarlo |
| Gráficas de alertas vacías | No hay alertas en `serving/alerts/` | Generar alertas y actualizar el dashboard en el navegador |

## Respaldo de la versión publicada (opcional)

```bash
aws quicksight describe-dashboard-definition --aws-account-id "$ACCOUNT" \
  --dashboard-id nexasecure-wids-soc-dev --query Definition > /app/docs/evidencias_capturas/consumo/dashboard_definition.json
```
