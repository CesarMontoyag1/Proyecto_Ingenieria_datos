# Amazon EMR + Apache Flink (NexaSecure WIDS - Sprint 2)

Este módulo gestiona el clúster transitorio de **Amazon EMR** con **Apache Flink** para la detección en tiempo real de anomalías Wi-Fi (SLA < 2s).

> **Aviso de Presupuesto (AWS Academy Learner Lab):**  
> El clúster está configurado con **AutoTerminationPolicy** (IdleTimeout de 30 minutos). Aún así, se recomienda encenderlo únicamente para las pruebas y terminarlo inmediatamente al finalizar.

---

## 1. Encender el Clúster

Antes de desplegar, subir el script de bootstrap a S3:
```bash
RAW_BUCKET=$(aws ssm get-parameter --name /nexasecure/dev/storage/RawAwid3BucketName --query Parameter.Value --output text)
aws s3 cp bootstrap/emr-bootstrap.sh s3://${RAW_BUCKET}/bootstrap/emr-bootstrap.sh
```

Desplegar el clúster EMR mediante Serverless Framework:
```bash
serverless deploy --stage dev
```

Obtener el `ClusterId` generado:
```bash
CLUSTER_ID=$(aws ssm get-parameter --name /nexasecure/dev/emr/ClusterId --query Parameter.Value --output text)
echo "EMR Cluster ID: $CLUSTER_ID"
```

---

## 2. Lanzar el Job de Detección (PyFlink Step)

1. Subir la aplicación Flink a S3:
```bash
aws s3 cp ../../../flink-app/ s3://${RAW_BUCKET}/flink-app/ --recursive
```

2. Ejecutar el Step en el clúster EMR:
```bash
aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps Type=CUSTOM_JAR,Name="NexaSecureFlinkDetectionStep",Jar="command-runner.jar",ActionOnFailure=CONTINUE,Args=["flink","run","-m","yarn-cluster","-py","/tmp/detection_job.py"]
```

O conectarse vía SSH al nodo Master y ejecutar manualmente:
```bash
flink run -m yarn-cluster -py /home/hadoop/flink-app/src/detection_job.py
```

---

## 3. Apagar / Terminar el Clúster

Una vez concluidas las pruebas de detección:

```bash
# Opción A: Terminar inmediatamente desde AWS CLI
aws emr terminate-clusters --cluster-ids $CLUSTER_ID

# Opción B: Destruir el stack de Serverless
serverless remove --stage dev
```
