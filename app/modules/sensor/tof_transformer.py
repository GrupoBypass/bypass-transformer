from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, col, mean, lit, when, count, sum as spark_sum
from pyspark.sql.types import IntegerType
from modules.cleaning.transformer import Transformer
import os
import boto3
from decimal import Decimal

class TofTransformer(Transformer):

    AWS_ACESS_KEY_ID = os.environ.get("$AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACESS_KEY = os.environ.get("$AWS_SECRET_ACCESS_KEY")
    AWS_SESSION_TOKEN = os.environ.get("$AWS_SESSION_TOKEN")
    
    def main(self, local_input, s3, key):
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/tof_trusted.csv"   # nome fixo
        bucket_trusted = os.environ.get("$S3_TRUSTED")
        
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

        dynamodb = boto3.resource(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="YOUR_ACCESS_KEY",
            aws_secret_access_key="YOUR_SECRET_KEY",
            aws_session_token="YOUR_SESSION_TOKEN"  # optional, only if using temporary credentials (e.g., STS)
        )
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        metadata_table = dynamodb.Table("SensorMetadata")
        tof_table = dynamodb.Table("TofData")
        
        result_df = (df
            .withColumn("is_occupied", when(col("dist_mm") < 1750, 1).otherwise(0))
            .groupBy("sensor_id", "timestamp")
            .agg(
                spark_sum("is_occupied").alias("occupied_points"),
                count(lit(1)).alias("total_points"),
                (spark_sum("is_occupied") / count(lit(1)) * 100).alias("ocupacao_media")
            )
        )
        
        for row in result_df.collect():
            sensor_value = row.get("sensor_id")
            sensor_id = f"S{sensor_value}"
            ocupacao = float(row["ocupacao_media"])
            timestamp = row["timestamp"]

            # Lookup sensor no DynamoDB
            meta = metadata_table.get_item(Key={"sensor_id": sensor_id})
            if "Item" not in meta:
                print(f"⚠️ Sensor {sensor_id} não encontrado em SensorMetadata")
                continue

            trem_id = meta["Item"]["trem_id"]
            carro_id = meta["Item"]["carro_id"]

            # Insere no DynamoDB (tabela TofData)
            tof_table.put_item(
                Item={
                    "trem_id": trem_id,
                    "datahora": timestamp,
                    "carro_id": carro_id,
                    "sensor_id": sensor_id,
                    "ocupacao": Decimal(str(ocupacao))
                }
            )
            print(f"✅ Inserido ToFData: {sensor_id} → {trem_id}/{carro_id} com ocupação {ocupacao:.2f}%")

    def tratar_dataframe_client(self, df: DataFrame, s3, key):
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        metadata_table = dynamodb.Table("SensorMetadata")
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_client = os.environ.get("$S3_CLIENT")

        df = (df
              .withColumn("y_block", floor(col("y") / 4).cast(IntegerType()))
              .withColumn("x_block", floor(col("x") / 4).cast(IntegerType()))
              .groupBy("sensor_id", "timestamp", "y_block", "x_block")
              .agg(
                  mean("dist_mm").alias("dist")
              )
              .select("y_block", "x_block", "dist", "timestamp", "sensor_id")
              .orderBy("y_block", "x_block")
              )
        
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
