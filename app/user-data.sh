#!/bin/bash
# Atualizar pacotes
apt update -y && apt upgrade -y

# Instalar dependências principais
apt install -y openjdk-11-jdk python3 python3-pip python3-venv git unzip curl awscli

# Atualizar pip e instalar bibliotecas necessárias
pip3 install --upgrade pip
pip3 install pyspark boto3 flask gunicorn requests pandas

# Criar diretórios padrão
mkdir -p /home/ubuntu/app /home/ubuntu/uploads /home/ubuntu/output
chown -R ubuntu:ubuntu /home/ubuntu/app /home/ubuntu/uploads /home/ubuntu/output

