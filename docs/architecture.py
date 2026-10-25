"""Generate PNG/SVG architecture diagrams from the repository root."""
from pathlib import Path
import base64
import os
import re
import shutil

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.analytics import Athena, EMRCluster, Glue, GlueDataCatalog, KinesisDataFirehose, KinesisDataStreams
from diagrams.aws.compute import Lambda
from diagrams.aws.database import RDSPostgresqlInstance
from diagrams.aws.integration import SNS, SQS
from diagrams.aws.security import SecretsManager
from diagrams.aws.storage import S3
from diagrams.generic.storage import Storage
from diagrams.programming.language import Python

if not shutil.which('dot') and os.name == 'nt':
    graphviz_bin = Path(os.environ.get('ProgramFiles', 'C:/Program Files')) / 'Graphviz/bin'
    if (graphviz_bin / 'dot.exe').is_file():
        os.environ['PATH'] = str(graphviz_bin) + os.pathsep + os.environ.get('PATH', '')
if not shutil.which('dot'):
    raise RuntimeError('Install Graphviz or run this script in the root DevContainer.')

output = Path(__file__).with_name("nexasecure_wids_-_arquitectura_streaming_&_batch")
with Diagram("NexaSecure WIDS - Arquitectura Streaming & Batch", filename=str(output),
             outformat=["png", "svg"], show=False, direction="LR",
             graph_attr={"nodesep": "0.9", "ranksep": "1.2", "fontname": "Arial"},
             node_attr={"fontname": "Arial", "fontsize": "12", "width": "1.7", "height": "2.5"},
             edge_attr={"fontname": "Arial", "fontsize": "9"}):
    source = Storage("AWID3 / Kaggle")
    batch_loader = Python("Batch loader\n50% por categoría")
    simulator = Python("Simulador streaming\n50% por categoría")
    source >> [batch_loader, simulator]

    with Cluster("S3 / Lakehouse"):
        landing = S3("Raw bucket\nlanding/batch/ (CSV)")
        raw_batch = S3("Raw bucket\nraw/ (Iceberg)")
        raw_stream = S3("Raw bucket\nraw/streaming/ (JSON)")
        trusted = S3("Trusted bucket / Iceberg\nFecha + Label\nOrden/Bloom BSSID")
        stream_trusted = S3("Trusted / streaming/ (NDJSON)\nIntegración Iceberg pendiente")
        quarantine = S3("Quarantine bucket\nBatch: Parquet / Streaming: JSON")
        baseline = S3("Trusted / benchmark/\nIceberg sin optimizar")

    with Cluster("Procesamiento batch / AWS Glue"):
        ingest = Glue("Validar y seudonimizar\nBatchIngestJob")
        maintenance = Glue("Mantenimiento\nCompactación + snapshots")
        optimization = Glue("Migración Trusted\nBaseline + reescritura")
        catalog = GlueDataCatalog("Glue Data Catalog")
    batch_loader >> landing >> ingest
    ingest >> [raw_batch, trusted, quarantine]
    maintenance >> Edge(style="dashed") >> trusted
    trusted >> Edge(label="copia de referencia") >> optimization
    optimization >> [baseline, trusted]
    [raw_batch, trusted, baseline] >> Edge(style="dotted") >> catalog

    with Cluster("Streaming / consumidor Python desplegable sobre EMR"):
        kinesis = KinesisDataStreams("Kinesis Data Streams")
        firehose = KinesisDataFirehose("Firehose")
        consumer = EMRCluster("EMR / Python + boto3\nValidación y detección\nPyFlink pendiente")
    simulator >> kinesis
    kinesis >> firehose >> raw_stream
    kinesis >> consumer
    consumer >> [stream_trusted, quarantine]

    with Cluster("Alertas y notificaciones"):
        queue = SQS("Alertas + DLQ")
        formatter = Lambda("alert_formatter\nReportBatchItemFailures")
        notifications = SNS("SNS / Email SOC\nSlack opcional")
    consumer >> queue >> formatter >> notifications

    with Cluster("Capa transaccional / RDS"):
        postgres = RDSPostgresqlInstance("PostgreSQL\nrule_catalog / alerts\nincident_cases / audit_log")
        credentials = SecretsManager("Credenciales RDS\nMaestra / analista / auditor")
    consumer >> Edge(style="dashed", label="INSERT alerts\nIntegración: Luis") >> postgres
    postgres >> Edge(style="dashed", label="umbrales al iniciar") >> consumer
    credentials >> Edge(style="dotted") >> postgres
    hmac = SecretsManager("Llave HMAC compartida")
    hmac >> Edge(style="dotted") >> ingest
    hmac >> Edge(style="dotted") >> consumer

    with Cluster("Consultas y evidencia"):
        athena = Athena("Athena engine 3\nBenchmark antes/después")
        future = Storage("Sprint final\nRedshift / dashboard")
    [trusted, baseline, catalog] >> athena
    athena >> Edge(style="dashed") >> future

# Graphviz uses absolute image paths in SVG by default. Embed icons for sharing.
svg_path = output.with_suffix('.svg')
svg = svg_path.read_text(encoding='utf-8')


def embed_icon(match):
    path = Path(match.group(1))
    if not path.is_file():
        return match.group(0)
    data = base64.b64encode(path.read_bytes()).decode('ascii')
    return f'xlink:href="data:image/png;base64,{data}"'


svg_path.write_text(re.sub(r'xlink:href="([^"]+)"', embed_icon, svg), encoding='utf-8')
