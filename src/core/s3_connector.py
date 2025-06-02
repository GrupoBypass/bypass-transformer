import boto3
import pandas as pd
from pyspark.sql import SparkSession, DataFrame

class S3Connector:
    def __init__(self):
        self.spark = SparkSession.builder.appName("S3App").getOrCreate()
        self.s3_client = boto3.client("s3")

    def read_from_s3(self, path: str) -> DataFrame:
        return self.spark.read.option("header", True).option("inferSchema", True).csv(path)

    def write_to_s3(self, df: DataFrame, path: str):
        df.write.mode("overwrite").option("header", True).csv(path)