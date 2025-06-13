from src.core.s3_connector import S3Connector
from src.core.transformer import Transformer

AWS_ACCESS_KEY_ID = 'ASIA2IC2FFOWPWKWFQHZ'
AWS_SECRET_ACCESS_KEY = 'D11GYZu+/FPsO7q+fMQPbiSVJ0CvXJtg4y3NecI1'
AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjECEaCXVzLXdlc3QtMiJHMEUCIGazfAs4thjQCqcPOcvVuB8Z4aW7qGo3WeYrk3UtKXZXAiEAztzHoeLTHmRxMAj2NYSwSEDt03Ctk/o2KugNBik6hKoqvwII+f//////////ARABGgw3MDQ1NjM3MTkwODQiDKDlbujp5Qg+DiHAoyqTAnqRGUiNynGaPwT8ZIS5ahTJeX0YHRbdhlelc3UbD4uVv719dbklAFr2L274spp+FaKm6ejM+ORrD/dk6raGYUSCAXn63ZIut8CGlG88tLlfa/Fyf8uM7Usa9x0iKxexij5ozvlzauNL2d75qk/sKOOdqvJwTO/YV99OjwLmyHKLbrM2WfecEJCTok1/kvGh1Pnaj4jvn98sHImx40pZR1EttlINWFwt2jBnbnx6bRLB+7Kx4ANdThpeXn/Yd1xuc/cijk1kUTH25OsnvJR9qbqgpw5eZk7KAhf77YEjb1wUHZ523GaV7vB02YGaMcliGX/y3e4SwMMnAFNEkYqJz9e6GOLhxs3M/gz8qkCRRFL2g1GuMIbdrcIGOp0BC8mwlw55R1FauyM/Z9bEoBbbdU+WhpVqcqkLZVwzOua2livEEehIfPksZ6qn3oFJDV6zE5/NJRDoXjr1HUctt1r/DUqUwOuEzWU/kYW3RksFZevx8lBB6FPDHzTjqJqZvL5/+I+0qM20kLL9JQ3+qrDmZec60tlsMMEaoQIRrNgqJaYfNatxfFsQLV7kcxzUfSTQcQ3QsOEhXja0KA=='
REGIAO = 'us-east-1'

BUCKET_RAW = 'bypass-teste-raw'
BUCKET_TRUSTED = 'bypass-teste-trusted'

ARQUIVO_ENTRADA = 'tof_sensor'
ARQUIVO_SAIDA = 'tof_sensor_trusted'

SENSOR = 'tof'

con = S3Connector(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=REGIAO
)

spark = con.spark

s3_input_path = f"s3a://{BUCKET_RAW}/data/{ARQUIVO_ENTRADA}.csv"
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path, sep=",")

df_raw.show(10)

df_trusted = Transformer.tratar_dataframe(df_raw)

s3_output_path = f"s3a://{BUCKET_TRUSTED}/data/{SENSOR}"
df_trusted.coalesce(1).write.mode("overwrite").option("header", "true").csv(s3_output_path, sep=",")
con.rename_spark_csv_output(
    bucket_name=BUCKET_TRUSTED,
    s3_dir=f"data/{SENSOR}",
    new_filename=f"{ARQUIVO_SAIDA}.csv"
)
