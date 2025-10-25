#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import json
import os
import sys
import boto3
import logging

from flask import Flask, request, jsonify
from modules.sensor.tof_transformer import TofTransformer
from modules.sensor.dht11_transformer import DHT11Transformer
from modules.sensor.omron_transformer import OmronTransformer
from modules.sensor.dps_transformer import DpsTransformer
from modules.sensor.piezo_transformer import PiezoTransformer
from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)


# Configure logging
logging.basicConfig(
    level=logging.INFO,                     # Log everything (INFO and above)
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]  # Output to stdout (Docker reads this)
)

logger = logging.getLogger(__name__)

app = Flask(__name__)

load_dotenv()

BUCKET_RAW = os.environ.get("S3_RAW")
LOCAL_INPUT_TOF = "/tmp/input_tof.csv"
LOCAL_INPUT_DPS = "/tmp/input_dps.csv"
LOCAL_INPUT_DHT11 = "/tmp/input_dht11.csv"
LOCAL_INPUT_OMRON = "/tmp/input_omron.csv"
LOCAL_INPUT_PIEZO = "/tmp/input_piezo.csv"
LOCAL_INPUT_OPTICAL = "/tmp/input_optical.csv"

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")

session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name="us-east-1"
        )
        
s3 = session.client("s3")

@app.route("/process-test", methods=["POST"])
def process_test():
    data = request.json or {}
    logger.info("Teste recebido:", data)
    return jsonify({"status": "ok", "payload": data})

@app.route("/process-tof", methods=["POST"])
def process_tof():
    try:
        data = request.json or {}
        key = data.get("key")
        
        logger.info("Iniciando download do S3...")
        s3.download_file(BUCKET_RAW, key, LOCAL_INPUT_TOF)
        logger.info(f"Arquivo baixado localmente: {LOCAL_INPUT_TOF}")
        
        if not key or "tof" not in key:
            return jsonify({"status": "error", "message": "Arquivo inválido"}), 400

        transformer = TofTransformer()
        transformer.main(LOCAL_INPUT_TOF, s3, key)
        
        return jsonify({"status": "ok", "message": "Processamento concluído"}), 200
    except Exception as e:
        logger.exception(f"Erro inesperado: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    
@app.route("/process-dps", methods=["POST"])
def process_dps():
    try:
        data = request.json or {}
        key = data.get("key")
        
        logger.info("Iniciando download do S3...")
        s3.download_file(BUCKET_RAW, key, LOCAL_INPUT_DPS)
        logger.info(f"Arquivo baixado localmente: {LOCAL_INPUT_DPS}")
        
        if not key or "dps" not in key:
            return jsonify({"status": "error", "message": "Arquivo inválido"}), 400

        transformer = DpsTransformer()
        transformer.main(LOCAL_INPUT_DPS, s3, key)
        
        return jsonify({"status": "ok", "message": "Processamento concluído"}), 200
    except Exception as e:
        logger.exception(f"Erro inesperado: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/process-dht11", methods=["POST"])
def process_dht():
    try:
        data = request.json or {}
        key = data.get("key")
        
        logger.info("Iniciando download do S3...")
        s3.download_file(BUCKET_RAW, key, LOCAL_INPUT_DHT11)
        logger.info(f"Arquivo baixado localmente: {LOCAL_INPUT_DHT11}")
        
        if not key or "dht" not in key:
            return jsonify({"status": "error", "message": "Arquivo inválido"}), 400

        transformer = DHT11Transformer()
        transformer.main(LOCAL_INPUT_DHT11, s3, key)
        
        return jsonify({"status": "ok", "message": "Processamento concluído"}), 200
    except Exception as e:
        logger.exception(f"Erro inesperado: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/process-piezo", methods=["POST"])
def process_piezo():
    try:
        data = request.json or {}
        key = data.get("key")
        
        logger.info("Iniciando download do S3...")
        s3.download_file(BUCKET_RAW, key, LOCAL_INPUT_PIEZO)
        logger.info(f"Arquivo baixado localmente: {LOCAL_INPUT_PIEZO}")
        
        if not key or "piezo" not in key:
            return jsonify({"status": "error", "message": "Arquivo inválido"}), 400

        transformer = PiezoTransformer()
        transformer.main(LOCAL_INPUT_PIEZO, s3, key)
        
        return jsonify({"status": "ok", "message": "Processamento concluído"}), 200
    except Exception as e:
        logger.exception(f"Erro inesperado: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    
@app.route("/process-omron", methods=["POST"])
def process_omron():
    try:
        data = request.json or {}
        key = data.get("key")
        
        logger.info("Iniciando download do S3...")
        s3.download_file(BUCKET_RAW, key, LOCAL_INPUT_OMRON)
        logger.info(f"Arquivo baixado localmente: {LOCAL_INPUT_OMRON}")
        
        if not key or "omron" not in key:
            return jsonify({"status": "error", "message": "Arquivo inválido"}), 400

        transformer = OmronTransformer()
        transformer.main(LOCAL_INPUT_OMRON, s3, key)
        
        return jsonify({"status": "ok", "message": "Processamento concluído"}), 200
    except Exception as e:
        logger.exception(f"Erro inesperado: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    load_dotenv()

    session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name="us-east-1"
        )
        
    s3 = session.client("s3")
    
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
