import json
import boto3
import os
from itertools import islice
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

region = os.getenv("AWS_REGION", "us-east-1")
dynamodb = boto3.client("dynamodb", region_name=region)

# Mapping between filenames and table names
FILES_TO_TABLES = {
    "circuito_dump.json": "Circuito",
    "piezo_sensor_distancia_dump.json": "PiezoSensorDistancia",
    "sensor_metadata_dump.json": "SensorMetadata",
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


for filename, table_name in FILES_TO_TABLES.items():
    path = os.path.join(data_dir, filename)

    if not os.path.exists(path):
        print(f"File not found: {path}")
        continue

    print(f"Uploading {filename} to table {table_name}...")

    with open(path, "r") as f:
        data = json.load(f)

    items = data.get("Items", [])
    if not items:
        print(f"No items found in {filename}")
        continue

    for batch in chunks(items, 25):  # DynamoDB batch write limit
        request_items = {
            table_name: [{"PutRequest": {"Item": item}} for item in batch]
        }
        dynamodb.batch_write_item(RequestItems=request_items)

    print(f"Finished uploading {len(items)} items to {table_name}")
    it = iter(it)
    while chunk := list(islice(it, size)):
        yield chunk

for filename, table_name in FILES_TO_TABLES.items():
    path = os.path.join(data_dir, filename)

    if not os.path.exists(path):
        print(f"File not found: {path}")
        continue

    print(f"Uploading {filename} → {table_name}")

    with open(path) as f:
        data = json.load(f)

    items = data.get("Items", [])
    if not items:
        print(f"No items found in {filename}")
        continue

    for batch in chunks(items, 25):  # DynamoDB batch write limit
        request_items = {table_name: [{"PutRequest": {"Item": item}} for item in batch]}
        dynamodb.batch_write_item(RequestItems=request_items)

    print(f"Finished uploading {len(items)} items to {table_name}")
