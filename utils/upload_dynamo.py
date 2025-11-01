import json
import boto3
import os
import codecs
from itertools import islice
from dotenv import load_dotenv

load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key_id = os.environ.get("AWS_SECRET_ACCESS_KEY_ID")
aws_session_token = os.environ.get("AWS_SESSION_TOKEN")

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key_id,
    aws_session_token=aws_session_token,
    region_name="us-east-1"
)

dynamodb = session.client("dynamodb")

FILES_TO_TABLES = {
    "circuito_dump.json": "Circuito",
    "piezo_sensor_distancia_dump.json": "PiezoSensorDistancia",
    "sensor_metadata_dump.json": "SensorMetadata",
    "linha_dump.json": "Linha",
    "trilho_dump.json": "Trilho"
}

data_dir = os.getenv("DATA_DIR", "./data")


def chunks(it, size):
    """Yield successive batches of size 'size' from iterable 'it'."""
    it = iter(it)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch


def load_json_file(path):
    try:
        with codecs.open(path, 'r', 'utf-8-sig') as json_file:  
            return json.load(json_file)
    except UnicodeDecodeError:
        with codecs.open(path, 'r', 'utf-16le') as json_file:  
            return json.load(json_file)


# Main upload loop
for filename, table_name in FILES_TO_TABLES.items():
    path = os.path.join(data_dir, filename)

    if not os.path.exists(path):
        print(f"File not found: {path}")
        continue

    print(f"Uploading {filename} to table {table_name}...")

    try:
        data = load_json_file(path)
    except Exception as e:
        print(f"Failed to load {filename}: {e}")
        continue

    items = data.get("Items", [])
    if not items:
        print(f"No items found in {filename}")
        continue

    for batch in chunks(items, 25):  # DynamoDB batch write limit
        request_items = {
            table_name: [{"PutRequest": {"Item": item}} for item in batch]
        }
        try:
            dynamodb.batch_write_item(RequestItems=request_items)
        except Exception as e:
            print(f"Failed to upload batch to {table_name}: {e}")
            continue

    print(f"Finished uploading {len(items)} items to {table_name}")
