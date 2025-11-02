from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, col, mean, lit, when, count, sum as spark_sum, concat
from pyspark.sql.types import IntegerType
from modules.cleaning.transformer import Transformer
import os
import boto3
from decimal import Decimal
from dotenv import load_dotenv

class TofTransformer(Transformer):

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
        local_output_file = "/tmp/tof_trusted.csv"   # nome fixo
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
        
        self.tratar_dataframe_registry(df, s3, spark)
        
        self.tratar_dataframe_client(df, s3, key)
        
        
    
    def tratar_dataframe_registry(self, df: DataFrame, s3, spark):

        # dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        metadata_table = self.dynamodb.Table("SensorMetadata")
        tof_table = self.dynamodb.Table("TofData")
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_client = os.environ.get("S3_CLIENT")
        file_name = 'ocupacao.csv'
        
        metadata = metadata_table.scan()["Items"]  # Busca tudo de uma vez
        metadata_df = spark.createDataFrame(metadata)
        metadata_df = metadata_df.withColumnRenamed("sensor_id", "sensor_id_str")
        
        result_df = (df
            .withColumn("is_occupied", when(col("dist_mm") < 1750, 1).otherwise(0))
            .groupBy("sensor_id", "timestamp")
            .agg(
                spark_sum("is_occupied").alias("occupied_points"),
                count(lit(1)).alias("total_points"),
                (spark_sum("is_occupied") / count(lit(1)) * 100).alias("ocupacao_media")
            )
        )
        
        result_df = result_df.withColumn("sensor_id_str", concat(lit("S"), col("sensor_id")))
        
        final_df = (
            result_df
            .join(metadata_df, "sensor_id_str", "left")
            .select(
                col("sensor_id_str").alias("sensor_id"),
                col("trem_id"),
                col("carro_id"),
                col("timestamp").alias("datahora"),
                col("ocupacao_media")
            )
        )
        
        final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(local_output_dir)
        
        # Renomeia o arquivo final para resultado.csv
        for file_name in os.listdir(local_output_dir):
            if file_name.endswith(".csv"):
                src_path = os.path.join(local_output_dir, file_name)
                os.rename(src_path, local_output_file)
                print(f"Arquivo final gerado: {local_output_file}")

        # Envia arquivo para o S3
        s3.upload_file(local_output_file, bucket_client, file_name)
        print(f"Arquivo enviado para: s3://{bucket_client}/{file_name}")
        
        # Insere no dynamodb
        for row in final_df.collect():
            tof_table.put_item(
                Item={
                    "sensor_id": row["sensor_id"],
                    "trem_id": row["trem_id"],
                    "carro_id": row["carro_id"],
                    "datahora": row["datahora"],
                    "ocupacao": Decimal(str(row["ocupacao"]))
                }
            ) 

    def tratar_dataframe_client(self, df: DataFrame, s3, key):
        metadata_table = self.dynamodb.Table("SensorMetadata")
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_client = os.environ.get("S3_CLIENT")

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
        df = (df
              .withColumn("trem_id", lit(trem_id))
              .withColumn("carro_id", lit(carro_id))
              .select("y_block", "x_block", "dist", "timestamp", "trem_id", "carro_id")
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
