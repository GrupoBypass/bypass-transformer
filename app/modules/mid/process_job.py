#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import boto3
import re
import tof_transformer
import dht11_transformer
import omron_transformer
import dps_transformer
import piezo_transformer

def main(key):
    local_input = "/tmp/input.csv"
    bucket_raw = "bucket-bypass-raw-teste"

    print("Iniciando download do S3...")
    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )
    s3 = session.client("s3")
    s3.download_file(bucket_raw, key, local_input)
    print(f"Arquivo baixado localmente: {local_input}")
    
    if "tof" in key:
        sensor_transformer = tof_transformer.TofTransformer()
    elif "dht11" in key:
        sensor_transformer = dht11_transformer.DHT11Transformer()
    elif "omron" in key:
        sensor_transformer = omron_transformer.OmronTransformer()
    elif "dps" in key:
        sensor_transformer = dps_transformer.DpsTransformer()
    elif "piezo" in key:
        sensor_transformer = piezo_transformer.PiezoTransformer()

    sensor_transformer.main(local_input, s3, key)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python process_job.py <key>")
        sys.exit(1)

    key = sys.argv[1]

    main(key)
