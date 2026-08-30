from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.storage import S3
from diagrams.aws.analytics import EMRCluster, Glue, GlueDataCatalog, GlueCrawlers
from diagrams.aws.integration import KinesisDataStreams, KinesisDataFirehose
from diagrams.custom import Custom

# Nota: este script asume que corre dentro del devcontainer 'img-root' con graphviz instalado
with Diagram("NexaSecure WIDS - Arquitectura Streaming & Batch", show=False, direction="LR"):
    
    with Cluster("Capa de Almacenamiento"):
        s3_raw = S3("Raw Bucket\n(S3)")
        s3_trusted = S3("Trusted Bucket\n(S3)")
        s3_quarantine = S3("Quarantine Bucket\n(S3)")
        
    with Cluster("Sprint 1: Pipeline Streaming [BUILT]"):
        sim = Custom("Simulador Python\nAWID3", "https://upload.wikimedia.org/wikipedia/commons/c/c3/Python-logo-notext.svg")
        kinesis = KinesisDataStreams("Kinesis\nData Streams")
        firehose = KinesisDataFirehose("Firehose")
        
        sim >> kinesis >> firehose >> s3_raw
        
    with Cluster("Sprint 1: Pipeline Batch Glue [BUILT]"):
        csv_local = Custom("Dataset AWID3\nCSV", "https://upload.wikimedia.org/wikipedia/commons/c/c3/Python-logo-notext.svg")
        glue_batch = Glue("Batch Ingest Job\n(PySpark)")
        glue_catalog = GlueDataCatalog("Data Catalog")
        glue_crawler = GlueCrawlers("Glue Crawler")
        
        csv_local >> glue_batch >> s3_raw
        glue_batch >> s3_trusted
        glue_batch >> s3_quarantine
        
        s3_raw >> Edge(style="dashed", color="gray") >> glue_crawler
        s3_trusted >> Edge(style="dashed", color="gray") >> glue_crawler
        glue_crawler >> glue_catalog
        
    with Cluster("Mantenimiento Iceberg [PENDING]"):
        glue_compaction = Glue("Iceberg Maint.\nJob")
        glue_compaction >> Edge(style="dashed") >> s3_trusted
        
    with Cluster("Sprint 2: Detección Streaming [PENDING]"):
        emr_flink = EMRCluster("EMR + Flink\n(Detección)")
        
        kinesis >> Edge(style="dashed", color="firebrick", label="Stream a evaluar") >> emr_flink
        emr_flink >> s3_trusted
        emr_flink >> s3_quarantine
