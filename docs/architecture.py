from diagrams import Diagram, Cluster, Edge

from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.analytics import (
    EMRCluster,
    Glue,
    GlueDataCatalog,
    GlueCrawlers,
    KinesisDataStreams,
    KinesisDataFirehose,
    Athena,
    Redshift,
    Quicksight,
)
from diagrams.aws.integration import SNS
from diagrams.programming.language import Python
from diagrams.generic.storage import Storage
from diagrams.onprem.analytics import PowerBI

# Nota: este script asume que corre dentro del devcontainer 'img-root' con graphviz instalado

with Diagram("NexaSecure WIDS - Arquitectura Streaming & Batch", show=False, direction="LR"):

    with Cluster("Capa de Almacenamiento"):
        s3_raw = S3("Raw Bucket\n(S3)")
        s3_trusted = S3("Trusted Bucket\n(S3)")
        s3_quarantine = S3("Quarantine Bucket\n(S3)")

    with Cluster("Sprint 1: Pipeline Streaming [BUILT]"):
        sim = Python("Simulador Python\nAWID3")
        kinesis = KinesisDataStreams("Kinesis\nData Streams")
        firehose = KinesisDataFirehose("Firehose")

        sim >> kinesis >> firehose >> s3_raw

    with Cluster("Sprint 1: Pipeline Batch Glue [BUILT]"):
        csv_local = Storage("Dataset AWID3\nCSV")
        glue_batch = Glue("Batch Ingest Job\n(PySpark)")
        glue_catalog = GlueDataCatalog("Data Catalog")
        glue_crawler = GlueCrawlers("Glue Crawler")

        csv_local >> glue_batch >> s3_raw
        glue_batch >> s3_trusted
        glue_batch >> s3_quarantine

        s3_raw >> Edge(style="dashed", color="gray") >> glue_crawler
        s3_trusted >> Edge(style="dashed", color="gray") >> glue_crawler
        glue_crawler >> glue_catalog

    with Cluster("Mantenimiento Iceberg [BUILT]"):
        glue_compaction = Glue("Iceberg Maint. Job\nCompactación + Snapshots")
        glue_compaction >> s3_trusted

    with Cluster("Sprint 2: Detección Streaming [PENDING]"):
        emr_flink = EMRCluster("EMR + Flink\n(Detección, SLA < 2s)")

        kinesis >> Edge(style="dashed", color="firebrick", label="Stream a evaluar") >> emr_flink
        emr_flink >> s3_trusted
        emr_flink >> s3_quarantine

    with Cluster("Sistema de Notificaciones [PENDING]"):
        lambda_alert = Lambda("Alert Formatter\n(Lambda)")
        sns_topic = SNS("Amazon SNS\nEmail / Slack SOC")

        emr_flink >> Edge(style="dashed", color="firebrick", label="Patrón anómalo") >> lambda_alert
        lambda_alert >> sns_topic

    with Cluster("Sprint 2: Capa Analítica [PENDING]"):
        athena = Athena("Amazon Athena\nConsulta ad-hoc")
        redshift = Redshift("Amazon Redshift\nSpectrum · SLA 500ms")

        s3_trusted >> Edge(style="dashed", color="firebrick") >> athena
        s3_trusted >> Edge(style="dashed", color="firebrick") >> redshift
        glue_catalog >> Edge(style="dashed", color="gray") >> athena
        glue_catalog >> Edge(style="dashed", color="gray") >> redshift

    with Cluster("Consumo y Vista [PENDING]"):
        quicksight = Quicksight("Amazon QuickSight")
        powerbi = PowerBI("Power BI")

        [athena, redshift] >> Edge(style="dashed", color="firebrick") >> quicksight
        [athena, redshift] >> Edge(style="dashed", color="firebrick") >> powerbi