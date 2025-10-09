from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from transformer import Transformer
import os
import boto3
from decimal import Decimal

class OpticalTransformer(Transformer):

    def main(self, local_input, s3, key):
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_trusted = "bucket-bypass-trusted-teste"
        
        print("Iniciando Spark...")
        spark = SparkSession.builder.appName("OpticalSpark").getOrCreate()
        
        print(f"Lendo CSV: {local_input}")
        df = spark.read.option("header", True).option("inferSchema", "true").csv(local_input)
        print(f"Linhas lidas: {df.count()}")
        
        df = super().tratar_dataframe(df)
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(local_output_dir)

        # Renomeia o arquivo final para resultado.csv
        for file_name in os.listdir(local_output_dir):
            if file_name.endswith(".csv"):
                src_path = os.path.join(local_output_dir, file_name)
                os.rename(src_path, local_output_file)
                print(f"Arquivo final gerado: {local_output_file}")

        # Envia arquivo para o S3
        s3.upload_file(local_output_file, bucket_trusted, key)
        print(f"Arquivo enviado para: s3://{bucket_trusted}/{key}")
        
        self.tratar_dataframe_registry(df)
        
        self.tratar_dataframe_client(df, s3, key)
        
        
    def tratar_dataframe_registry(self, df: DataFrame) -> DataFrame:
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        metadata_table = dynamodb.Table("SensorMetadata")
        dht11_table = dynamodb.Table("DHT11Data")
        
        for row in df.collect():
            sensor_id = f"S{row["sensor_id"]}"
            temperatura = float(row["temperature_c"])
            umidade = float(row["humidity_percent"])
            timestamp = row["timestamp"]

            # Lookup sensor no DynamoDB
            meta = metadata_table.get_item(Key={"sensor_id": sensor_id})
            if "Item" not in meta:
                print(f"⚠️ Sensor {sensor_id} não encontrado em SensorMetadata")
                continue

            trem_id = meta["Item"]["trem_id"]
            carro_id = meta["Item"]["carro_id"]

            # Insere no DynamoDB (tabela TofData)
            dht11_table.put_item(
                Item={
                    "trem_id": trem_id,
                    "datahora": timestamp,
                    "carro_id": carro_id,
                    "sensor_id": sensor_id,
                    "temperatura": Decimal(str(temperatura)),
                    "umidade": Decimal(str(umidade))
                }
            )
            print(f"✅ Inserido DHT11Data: {sensor_id} → {trem_id}/{carro_id} com temperatura {temperatura:.2f}ºC e umidade {umidade:.2f}%")

    def tratar_dataframe_client(self, df: DataFrame, s3, key):
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        metadata_table = dynamodb.Table("SensorMetadata")
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_client = "bucket-bypass-client-teste"

        # Pega o primeiro sensor_id (assume que só tem um sensor no arquivo)
        sensor_id = f"S{df.select('sensor_id').first()['sensor_id']}"

        meta = metadata_table.get_item(Key={"sensor_id": sensor_id})
        
        if "Item" not in meta:
            print(f"⚠️ Sensor {sensor_id} não encontrado em SensorMetadata")
            return

        trem_id = meta["Item"]["trem_id"]
        carro_id = meta["Item"]["carro_id"]

        # Em cada linha adiciona trem_id e carro_id
        df = df.withColumn("trem_id", lit(trem_id)).withColumn("carro_id", lit(carro_id))
        
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(local_output_dir)

        # Renomeia o arquivo final para resultado.csv
        for file_name in os.listdir(local_output_dir):
            if file_name.endswith(".csv"):
                src_path = os.path.join(local_output_dir, file_name)
                os.rename(src_path, local_output_file)
                print(f"Arquivo final gerado: {local_output_file}")

        # Envia arquivo para o S3
        s3.upload_file(local_output_file, bucket_client, key)
        print(f"Arquivo enviado para: s3://{bucket_client}/{key}")
