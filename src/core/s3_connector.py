import boto3
import pandas as pd
import io
from typing import Optional, Union
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

class S3Connector:
    def __init__(self,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 aws_session_token: Optional[str] = None,
                 region_name: str = 'us-west-2'):

        self.spark = SparkSession.builder \
            .appName("S3App") \
            .config("spark.jars.packages", 
                    "org.apache.hadoop:hadoop-aws:3.3.1,"
                    "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
            .config("spark.hadoop.fs.s3a.metrics.mode", "none") \
            .getOrCreate()
            # .config("spark.jars", "hadoop-aws-3.2.0.jar,aws-java-sdk-bundle-1.11.1026.jar") \

        print("Configurações Spark:")
        for k, v in self.spark.sparkContext.getConf().getAll():
            if "timeout" in k.lower() or "s3a" in k.lower():
                print(f"{k} = {v}")

        # print("\nConfigurações Hadoop:")
        # conf = self.spark._jsc.hadoopConfiguration()
        # for item in conf.iterator():
        #     k, v = item.key(), item.value()
        #     if "timeout" in k.lower() or "s3a" in k.lower() or "60s" in v:
        #         print(f"{k} = {v}")

        self.s3_client = boto3.client(
            's3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

    def get_file_from_s3(self, file_name: str, bucket_name: str = 'bypass-trusted') -> pd.DataFrame:
        try:
            file_key = f"data/{file_name}.csv"
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            body = response['Body']
            return pd.read_csv(body)

        except self.s3_client.exceptions.NoSuchKey:
            print(f"Arquivo '{file_name}' não encontrado no bucket.")
        except Exception as e:
            print(f"Erro ao acessar o S3: {e}")
        
        return pd.DataFrame()  

    def write_file_to_s3(self,
                         df: Union[pd.DataFrame, SparkDataFrame],
                         file_name: str,
                         bucket_name: str = 'bypass-trusted') -> None:
        try:
            file_key = f"data/{file_name}.csv"

            if isinstance(df, pd.DataFrame):
                buffer = io.StringIO()
                df.to_csv(buffer, index=False)
                buffer.seek(0)
                self.s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=buffer.getvalue())

            elif isinstance(df, SparkDataFrame):
                s3_path = f"s3a://{bucket_name}/data/{file_name}"
                df.write.mode('overwrite').csv(s3_path, header=True)
            else:
                print("Tipo de DataFrame não suportado.")

            print(f"Arquivo '{file_name}.csv' salvo com sucesso no bucket '{bucket_name}'.")

        except Exception as e:
            print(f"Erro ao salvar o arquivo no S3: {e}")
