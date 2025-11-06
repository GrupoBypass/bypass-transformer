import json
import urllib3
import os

http = urllib3.PoolManager()

def lambda_handler(event, context):
    try:
        TRANSFORMER_EC2_PUBLIC_IP = os.environ.get("TRANSFORMER_EC2_PUBLIC_IP")
        
        # --- Extrair o nome do arquivo e o bucket do evento S3 ---
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']

        # --- Montar o payload do POST ---
        payload = {
            "key": file_key
        }

        sensor_route = get_route(file_key)

        # --- Fazer o POST para sua EC2 ---
        url = f"http://{TRANSFORMER_EC2_PUBLIC_IP}:5000/{sensor_route}"
        headers = {"Content-Type": "application/json"}
        
        response = http.request(
            "POST",
            url,
            body=json.dumps(payload).encode("utf-8"),
            headers=headers
        )

        # --- Retornar resposta ---
        return {
            "statusCode": response.status,
            "body": response.data.decode("utf-8")
        }

    except Exception as e:
        print("Erro ao processar evento:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"erro": str(e)})
        }

def get_route(key):    
    if "dht" in key:
        return "process-dht11"
    elif "dps" in key:
        return "process-dps"
    elif "optical" in key:
        return "process-optical"
    elif "omron" in key:
        return "process-omron"
    elif "piezo" in key:
        return "process-piezo"
    elif "tof" in key:
        return "process-tof"
    
