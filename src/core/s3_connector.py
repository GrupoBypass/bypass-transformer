import boto3
import io
from typing import Optional, Union
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

class S3Connector:
    def __init__(self,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 aws_session_token: Optional[str] = None,
                 region_name: str = 'us-west-2'):

        jdbc_jar_path = "./jars/mysql-connector-j-9.3.0.jar"
        
        builder = SparkSession.builder \
            .appName("S3App") \
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.4.0,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.698") \
            .config("spark.driver.memory", "1g") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region_name}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
            .config("spark.hadoop.fs.s3a.metrics.mode", "none") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
            .config("spark.jars", jdbc_jar_path) \
            

        # Adiciona credenciais temporárias apenas se houver session token
        if aws_session_token:
            builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
                             .config("spark.hadoop.fs.s3a.session.token", aws_session_token)
        else:
            builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        self.spark = builder.getOrCreate()

        self.s3_client = boto3.client(
            's3',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

    def get_file_from_s3(self, file_name: str, bucket_name: str) -> SparkDataFrame:
        try:
            s3_input_path = f"s3a://{bucket_name}/data/{file_name}.csv"
            return self.spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path, sep=",")
        except self.s3_client.exceptions.NoSuchKey:
            print(f"Arquivo '{file_name}' não encontrado no bucket.")
        except Exception as e:
            print(f"Erro ao acessar o S3: {e}")
        return SparkDataFrame

    def write_file_to_s3(self,
                         df: SparkDataFrame,
                         sensor: str,
                         bucket_name: str,
                         file_name: str) -> None:
        try:
            s3_path = f"s3a://{bucket_name}/data/{sensor}"
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(s3_path, sep=",")
            df.write.mode('overwrite').option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(s3_path, header=True)
            self.rename_spark_csv_output(bucket_name, f"data/{sensor}", f"{file_name}.csv")
        except self.s3_client.exceptions.NoSuchBucket:
            print(f"Bucket '{bucket_name}' não encontrado.")
        except Exception as e:
            print(f"Erro ao salvar o arquivo no S3: {e}")
    
    def rename_spark_csv_output(self, bucket_name: str, s3_dir: str, new_filename: str):
        """
        Renomeia o arquivo part-*.csv gerado pelo Spark para um nome definido pelo usuário.
        Exemplo de uso:
            con.rename_spark_csv_output('meu-bucket', 'data/tof_sensor_trusted', 'tof_sensor_trusted.csv')
        """

        # Lista arquivos no diretório de saída
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{s3_dir}/")
        part_file = None
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv') and 'part-' in key:
                part_file = key
                break

        if not part_file:
            print("Arquivo part-*.csv não encontrado no diretório de saída.")
            return

        # Copia o arquivo para o novo nome
        copy_source = {'Bucket': bucket_name, 'Key': part_file}
        dest_key = f"{s3_dir}/{new_filename}"
        self.s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=dest_key)
        print(f"Arquivo renomeado para {dest_key}")

        # (Opcional) Remove arquivos antigos e _SUCCESS
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.startswith(f"{s3_dir}/") and (key.endswith('.csv') or key.endswith('_SUCCESS')):
                if key != dest_key:
                    self.s3_client.delete_object(Bucket=bucket_name, Key=key)