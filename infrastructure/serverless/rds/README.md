# RDS PostgreSQL — entregable de Daniel

Stack Serverless Framework 3 para una cuenta AWS normal, región `us-east-1`.
PostgreSQL 16, `db.t4g.micro`, 20 GiB gp3 cifrados, una AZ y un día de respaldos.
CloudFormation conserva un snapshot al eliminar o reemplazar la instancia.

## Preparación y despliegue

Desde la raíz, en PowerShell:

```powershell
py -3.12 -m venv .venv
.venv/Scripts/python.exe -m pip install -r scripts/requirements.txt
npm ci --prefix infrastructure --ignore-scripts
.venv/Scripts/python.exe scripts/rds.py prepare
.venv/Scripts/python.exe scripts/rds.py package
.venv/Scripts/python.exe scripts/rds.py deploy
.venv/Scripts/python.exe scripts/rds.py status
.venv/Scripts/python.exe scripts/rds_migrate_remote.py
```

Los scripts leen `.envs/aws.env` sin imprimir sus valores. `prepare` descubre
la VPC predeterminada, subredes, grupo `ElasticMapReduce-master`, una versión
PostgreSQL 16 disponible. Guarda únicamente
la configuración de despliegue en `.envs/rds-dev.json`, ignorada por Git.
Revisar ese JSON antes de desplegar en otra cuenta. `--stage` selecciona otro entorno.

## Red y permisos

La instancia es privada (`PubliclyAccessible: false`). El puerto 5432 admite
únicamente el grupo de seguridad del nodo principal de EMR. Aunque las subredes
existentes tengan ruta al Internet Gateway, RDS no recibe una dirección pública.
No se crean roles IAM de personas ni se modifican los roles de Sebastián.

`rds_migrate_remote.py` crea una instancia EC2 temporal `t3.micro` con SSM,
IMDSv2 obligatorio, disco cifrado y ningún puerto de entrada. Autoriza su grupo
de seguridad a conectarse a PostgreSQL y aplica las migraciones por la red privada.
Su rol temporal solo obtiene los dos parámetros RDS necesarios, las tres
credenciales de migración y los permisos del agente SSM. No copia credenciales AWS.
Al terminar, incluso si falla la migración, elimina el stack temporal: instancia,
perfil, rol y regla adicional de PostgreSQL. Una interrupción abrupta del equipo
puede impedir el `finally`: comprobar y eliminar el stack
`nexasecure-migration-runner-dev` si quedó pendiente.

La ejecución directa `scripts/rds.py migrate` queda disponible desde un entorno
que ya tenga conectividad privada con la VPC. No funciona directamente por Internet.

Las contraseñas maestra, de `security_analyst` y de `auditor` se generan en
Secrets Manager. El script de migración verifica TLS con el certificado de AWS,
aplica los SQL originales de César en orden y registra checksum/fecha en
`schema_migrations`. Un segundo `migrate` omite los ya aplicados; si cambia una
migración aplicada, aborta. Cada archivo y su registro se confirman en la misma
transacción, con un bloqueo asesor para evitar dos migradores simultáneos.

## Entrega a Luis

Parámetros publicados en `/nexasecure/dev/rds/`:

| Parámetro | Uso |
|---|---|
| `Endpoint` | Host PostgreSQL |
| `Port` | Puerto 5432 |
| `DatabaseName` | `nexasecure` |
| `MasterSecretArn` | Aplicar migraciones; no usar como identidad del consumidor |
| `SecurityGroupId` | Control de conectividad |

Los secretos de los perfiles PostgreSQL se llaman
`/nexasecure/dev/rds/SecurityAnalystCredentials` y
`/nexasecure/dev/rds/AuditorCredentials`.
El consumidor necesita su propia identidad con `SELECT` en `rule_catalog`,
`INSERT` en `alerts` y uso de la secuencia; su integración corresponde a Luis.
`security_analyst` es un perfil humano y no tiene permiso para insertar alertas.

No hay población sintética automática de `alerts`. El benchmark de índices de
César debe ejecutarse cuando Luis haya generado datos. `demo_indexes.py` elimina
y recrea índices para comparar; coordinar su ejecución con César.
