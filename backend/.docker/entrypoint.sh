#!/bin/bash
# Configurar git si variables están presentes
if [ -n "$GIT_USER_NAME" ] && [ -n "$GIT_USER_EMAIL" ]; then
    git config --global user.name "$GIT_USER_NAME"
    git config --global user.email "$GIT_USER_EMAIL"
fi

# Instalar dependencias npm (Serverless plugins)
if [ -f "package.json" ]; then
    echo "Instalando dependencias npm..."
    npm install
fi

# Instalar dependencias de Python para layers
for req in $(find src/layers -name requirements.txt); do
    echo "Instalando dependencias para $req"
    pip install -r $req -t $(dirname $req)/python
done

# Configurar AWS
if [ -f "/app/.envs/aws.env" ]; then
    mkdir -p ~/.aws
    cat <<EOF > ~/.aws/credentials
[default]
aws_access_key_id = $AWS_ACCESS_KEY_ID
aws_secret_access_key = $AWS_SECRET_ACCESS_KEY
aws_session_token = $AWS_SESSION_TOKEN
EOF
    cat <<EOF > ~/.aws/config
[default]
region = ${AWS_DEFAULT_REGION:-us-east-1}
output = ${AWS_DEFAULT_OUTPUT:-json}
EOF
    echo "Identidad de AWS configurada:"
    aws sts get-caller-identity
else
    echo "Advertencia: No se encontró /app/.envs/aws.env"
fi

