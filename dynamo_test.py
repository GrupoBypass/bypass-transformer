import boto3
from botocore.exceptions import ClientError

# Configuração (ajuste a região para a sua)
REGION = "us-east-1"
TABLE_NAME = "SensorMetadata"

# Criar cliente
dynamodb = boto3.resource(
    "dynamodb",
    region_name=REGION
)

table = dynamodb.Table(TABLE_NAME)

def write_item(item_id, data):
    try:
        response = table.put_item(
            Item={
                "sensor_id": "S1",
                "train_id": "T1",
                "car_number": "C1"
            }
        )
        print(f"✅ Item inserido: {item_id}")
        return response
    except ClientError as e:
        print(f"❌ Erro ao inserir: {e.response['Error']['Message']}")

def read_item(item_id):
    try:
        response = table.get_item(Key={"id": item_id})
        if "Item" in response:
            print(f"📖 Item lido: {response['Item']}")
            return response["Item"]
        else:
            print("⚠️ Item não encontrado")
            return None
    except ClientError as e:
        print(f"❌ Erro ao ler: {e.response['Error']['Message']}")

if __name__ == "__main__":
    # Teste de escrita
    write_item("123", "meu dado de teste")

    # Teste de leitura
    read_item("123")
