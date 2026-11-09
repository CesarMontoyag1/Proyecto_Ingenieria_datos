# CI/CD con GitHub Actions

El workflow está en [`.github/workflows/ci-cd.yml`](../../../.github/workflows/ci-cd.yml).
Este stack (`nexasecure-cicd-{stage}`) crea el rol de AWS que el workflow asume con OIDC,
así que en GitHub no se guarda ninguna llave ni hay que configurar *secrets*.

## Qué hace el pipeline

| Evento | Trabajos |
|---|---|
| Push a cualquier rama o pull request hacia `main` o `dev` | **CI**: pruebas de `flink-app`, `backend`, `ingestion-tools`, `scripts` y `glue-jobs`, y verificación de que el handler probado es el que se despliega |
| Push a `dev` o `main` (por ejemplo, al hacer merge de un PR) | **CI + CD**: si las pruebas pasan, despliega solo lo que cambió |
| Ejecución manual (Actions → CI/CD → Run workflow, rama `dev` o `main`) | **CI + CD** de todos los componentes |

Las dos ramas despliegan en el mismo ambiente de AWS (stage `dev`), que es el único que
existe en el proyecto.

Despliegue (CD):

| Componente | Cambia si se modifica | Cómo se despliega |
|---|---|---|
| Lambda `nexasecure-alerts-dev-alertFormatter` | `backend/src/alert_formatter/` | Sube `handler.py` y ejecuta una prueba de humo: envía un mensaje inválido y exige que la Lambda lo rechace sin publicar en SNS |
| Detector en EMR | `flink-app/src/` | Sube el código a `s3://<raw>/flink-app/` y, si el clúster está activo, reinicia el paso `NexaSecureDetectionJob` |
| Jobs de Glue | `glue-jobs/src/` | Sube los scripts a `s3://<raw>/glue-jobs/`; Glue los usa en la siguiente ejecución |

El CD despliega **código**. Los cambios de infraestructura (archivos `serverless.yml` y
plantillas) se siguen desplegando con `serverless deploy` desde el contenedor de
`infrastructure`, porque esos stacks leen correos y configuración de `.envs/`, que no
existe en GitHub.

## Permisos del rol

`nexasecure-github-actions-dev` solo puede:

- Leer parámetros SSM `/nexasecure/dev/*`.
- Actualizar el código e invocar la Lambda `nexasecure-alerts-dev-alertFormatter`.
- Escribir en los prefijos `flink-app/` y `glue-jobs/` del bucket Raw.
- Consultar, cancelar y agregar pasos en clústeres EMR.

Solo lo puede asumir un workflow que corra en las ramas `main` o `dev` (`DEPLOY_BRANCHES`)
de los repositorios de `GITHUB_REPOS`. No tiene acceso a otros recursos de la cuenta.

## Despliegue del stack (una sola vez)

Desde el contenedor de `infrastructure`:

```bash
# 1. ¿Ya existe el proveedor OIDC de GitHub en la cuenta?
aws iam list-open-id-connect-providers --query "OpenIDConnectProviderList[].Arn" --output text
#    Si la salida contiene token.actions.githubusercontent.com:  export CREATE_OIDC_PROVIDER=false

# 2. Desplegar
cd /app/infrastructure/serverless/cicd
serverless deploy --stage dev
```

Variables opcionales:

| Variable | Por defecto | Uso |
|---|---|---|
| `GITHUB_REPOS` | `CesarMontoyag1/Proyecto_Ingenieria_datos,luisNP21/Proyecto_Ingenieria_datos` | Repositorios (owner/repo) que pueden desplegar |
| `DEPLOY_BRANCHES` | `main,dev` | Ramas desde las que se puede desplegar |
| `CREATE_OIDC_PROVIDER` | `true` | `false` si la cuenta ya tiene el proveedor OIDC de GitHub |
| `GIT_USER_NAME` | `local` | Sufijo del bucket Raw |

El ARN del rol queda en `/nexasecure/dev/cicd/GitHubActionsRoleArn`. Si cambia la cuenta
o el stage, hay que actualizar `AWS_ROLE_ARN` en el workflow.

## Demo para el video

1. Crear una rama, cambiar algo visible en la Lambda (por ejemplo el asunto del correo en
   `backend/src/alert_formatter/handler.py`) y copiar el mismo archivo en
   `backend/serverless/alerts/handler.py`. Si las copias difieren, el CI falla a propósito.
2. Hacer push y abrir un pull request hacia `main`: se ven las pruebas corriendo.
3. Hacer merge: se ejecuta el despliegue y la prueba de humo.
4. Enviar un ataque con el simulador y mostrar el correo con el cambio.

La pestaña **Actions** del repositorio muestra cada ejecución, y el resumen de cada
una incluye la tabla de pruebas y lo que se desplegó. Esas pantallas sirven como
evidencia de DataOps para el informe.
