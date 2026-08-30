# NexaSecure WIDS - Ingestion Tools

Este subproyecto contiene herramientas de desarrollo local para alimentar la plataforma NexaSecure WIDS con datos reales del dataset AWID3 desde Kaggle, sin necesidad de descargar el dataset completo (46 GB) ni agotar los recursos locales o en AWS.

**Nota:** Este proyecto no se despliega a AWS. Corre exclusivamente en tu máquina de desarrollo.

## Prerrequisitos (Kaggle API)

Debes tener una cuenta en Kaggle y generar un token de API:
1. Ve a [Kaggle](https://www.kaggle.com/) -> Settings -> Create New Token.
2. Esto descargará un archivo `kaggle.json`.
3. Edita o crea el archivo `.envs/kaggle.env` en la raíz del monorepo con el siguiente contenido:

```env
KAGGLE_USERNAME=tu_usuario
KAGGLE_KEY=tu_token
```

## Uso

Levanta el contenedor de este proyecto:

```bash
cd ingestion-tools/.docker
docker-compose up -d
docker exec -it NexaSecureIngestionTools /bin/sh
```

### 1. Batch Loader (Subir CSVs a la zona Landing)

Descarga un subconjunto del dataset AWID3 y lo sube directamente al bucket S3 crudo, en la carpeta `landing/batch/`. El script usa los parámetros exportados por AWS SSM para descubrir el nombre del bucket automáticamente.

**Ejemplo de uso:**
```bash
python -m src.kaggle_batch_loader --kaggle-path "Deauth" --limit-files 1
```

> **Advertencia**: ¡NUNCA omitas el argumento `--kaggle-path`! Si lo haces, Kaggle intentará descargar los 46 GB completos del dataset.

### 2. Stream Simulator (Simular tráfico en tiempo real)

Descarga un CSV de Kaggle y simula que los eventos están ocurriendo en tiempo real, inyectándolos uno por uno en Kinesis Data Streams.

**Ejemplo de uso (Modo Dry-Run, no consume AWS):**
```bash
python -m src.kaggle_stream_simulator --kaggle-path "Normal_Traffic" --limit 20 --dry-run
```

**Ejemplo de uso (Envío a Kinesis real):**
```bash
python -m src.kaggle_stream_simulator --kaggle-path "Deauth" --limit 500 --rate 15.0
```

