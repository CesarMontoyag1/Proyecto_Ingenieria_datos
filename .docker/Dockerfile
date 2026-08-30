# Etapa 1: Base común
FROM ubuntu:22.04 AS img-base

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    git \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Instalar Node.js y npm (para Serverless Framework)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g --ignore-scripts serverless

# Instalar AWS CLI v2
RUN curl -fsSL --retry 5 --retry-connrefused --retry-delay 2 "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf aws awscliv2.zip

# Etapa 2: Infraestructura (Igual a base)
FROM img-base AS img-infrastructure
WORKDIR /app

# Etapa 3: Backend
FROM img-base AS img-backend
RUN pip3 install boto3 pandas fastparquet
WORKDIR /app

# Etapa 4: Root (Para diagramas)
FROM img-base AS img-root
RUN apt-get update && apt-get install -y graphviz && rm -rf /var/lib/apt/lists/*
RUN pip3 install diagrams
WORKDIR /app

