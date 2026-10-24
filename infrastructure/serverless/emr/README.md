# Amazon EMR + Apache Flink (NexaSecure WIDS - Sprint 2)

Este módulo gestiona el clúster transitorio de **Amazon EMR** con **Apache Flink** para la detección en tiempo real de anomalías Wi-Fi (SLA < 2s).

> **Aviso de Presupuesto (AWS Academy Learner Lab):**  
> El clúster está configurado con **AutoTerminationPolicy** (IdleTimeout de 30 minutos). Aún así, se recomienda encenderlo únicamente para las pruebas y terminarlo inmediatamente al finalizar.

---

## 1. Encender y Desplegar el Clúster

El despliegue es **100% automático** mediante el plugin `serverless-s3-sync`: antes de crear el clúster, Serverless sincroniza automáticamente a S3:
- El script de bootstrap (`bootstrap/emr-bootstrap.sh` -> `s3://.../bootstrap/`)
- La aplicación Flink completa (`flink-app/` -> `s3://.../flink-app/`)

Para desplegar:
```bash
cd /app/infrastructure/serverless/emr
npm install
serverless deploy --stage dev
```

Obtener el `ClusterId` generado:
```bash
CLUSTER_ID=$(aws ssm get-parameter --name /nexasecure/dev/emr/ClusterId --query Parameter.Value --output text)
echo "EMR Cluster ID: $CLUSTER_ID"
```

---

## 2. Lanzar el Job de Detección (PyFlink Step)

Ejecutar el Step en el clúster EMR:
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
