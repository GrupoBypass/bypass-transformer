from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
from modules.cleaning.transformer import Transformer
import os
import boto3
from decimal import Decimal
from dotenv import load_dotenv

class DpsTransformer(Transformer):
    
    def __init__(self):
        load_dotenv()
        AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY_ID = os.environ.get("AWS_SECRET_ACCESS_KEY_ID")
        AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")
        
        self.dynamodb = boto3.resource(
                "dynamodb",
                region_name="us-east-1",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID,
                aws_session_token=AWS_SESSION_TOKEN
        )

    def main(self, local_input, s3, key):
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_trusted = os.environ.get("S3_TRUSTED")
        
        print("Iniciando Spark...")
        spark = SparkSession.builder.appName("TofSpark").getOrCreate()
        
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
        
        
    
    def tratar_dataframe_registry(self, df: DataFrame):
        metadata_table = self.dynamodb.Table("SensorMetadata")
        dps_table = self.dynamodb.Table("DpsData")
        
        for row in df.collect():
            sensor_value = row["sensor_id"]
            sensor_id = f"S{sensor_value}"
            status = row["statusDPS"]
            tensao = float(row["picoTensao_kV"])
            corrente = float(row["correnteSurto_kA"])
            timestamp = row["dataHora"]

            # Lookup sensor no DynamoDB
            meta = metadata_table.get_item(Key={"sensor_id": sensor_id})
            if "Item" not in meta:
                print(f"⚠️ Sensor {sensor_id} não encontrado em SensorMetadata")
                continue

            # Insere no DynamoDB (tabela TofData)
            dps_table.put_item(
                Item={
                    "status": status,
                    "datahora": timestamp,
                    "tensao": Decimal(str(tensao)),
                    "corrente": Decimal(str(corrente)),
                    "sensor_id": sensor_id
                }
            )
            print(f"✅ Inserido DpsData: {sensor_id} com status {status}")

    def tratar_dataframe_client(self, df: DataFrame, s3, key):
        metadata_table = self.dynamodb.Table("SensorMetadata")
        circuito_table = self.dynamodb.Table("Circuito")
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_client = os.environ.get("S3_CLIENT")
    
        # Pega o primeiro sensor_id (assume que só tem um sensor no arquivo)
        sensor_id = f"S{df.select('sensor_id').first()['sensor_id']}"
        
        meta = metadata_table.get_item(Key={"sensor_id": sensor_id})
        
        if "Item" not in meta:
            print(f"⚠️ Sensor {sensor_id} não encontrado em SensorMetadata")
            return

        circuito = circuito_table.get_item(Key={"sensor_id": sensor_id})
        
        if "Item" not in circuito:
            print(f"⚠️ Circuito do sensor {sensor_id} não encontrado em Circuito")
            return
        
        trem_id = meta["Item"]["trem_id"]
        carro_id = meta["Item"]["carro_id"]
        circuito_id = circuito["Item"]["circuito_id"]
        circuito_modelo = circuito["Item"]["modelo"]
        circuito_categoria = circuito["Item"]["categoria"]
        circuito_prioridade = circuito["Item"]["prioridade"]

        df = (df
            .withColumn("NUM_TREM", lit(trem_id))
            .withColumn("NUM_CARRO", lit(carro_id))
            .withColumn("ID_CIRCUITO", lit(circuito_id))
            .withColumn("MODELO", lit(circuito_modelo))
            .withColumn("CATEGORIA", lit(circuito_categoria))
            .withColumn("PRIORIDADE", lit(circuito_prioridade))
            .withColumnRenamed("statusDPS", "STATUS")
            .withColumnRenamed("dataHora", "DATAHORA")
            .select("ID_CIRCUITO", "MODELO", "CATEGORIA", "PRIORIDADE", "STATUS", "NUM_CARRO", "NUM_TREM", "DATAHORA", "picoTensao_kV", "correnteSurto_kA")
        )
        
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
