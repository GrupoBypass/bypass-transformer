from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from modules.cleaning.transformer import Transformer
import os
import boto3
from decimal import Decimal

class OmronTransformer(Transformer):

    def main(self, local_input, s3, key):
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_trusted = os.environ.get("S3_TRUSTED")
        
        print("Iniciando Spark...")
        spark = SparkSession.builder.appName("DHT11Spark").getOrCreate()
        
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
        print("Sem implmentação")

    def tratar_dataframe_client(self, df: DataFrame, s3, key):
        # dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        # metadata_table = dynamodb.Table("SensorMetadata")
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_client = os.environ.get("S3_CLIENT")

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
