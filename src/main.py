from core.s3_connector import S3Connector
from pyspark.sql.functions import col
import os

os.environ['_JAVA_OPTIONS'] = '-Xmx1g'

AWS_ACCESS_KEY_ID = 'ASIA6L3Q6UCNBNCBFKPU'
AWS_SECRET_ACCESS_KEY = '2B3qrlvixkIo143E9X1o5UvbjHd7dwOf4u14Xj6z'
AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEGAaCXVzLXdlc3QtMiJHMEUCIQCvtOSulksed4TPQ50zMPB1gVSFicLO/9XQGCtbKSB/jQIgezdBNtcqNO4TXmR1U3+Sz8oYx+mUM/CT51wM/4el3uAqtAIIORAAGgw5ODc1NDI0OTUzODYiDLmjpDMdqwv6E9KGayqRAvyc8R2vXmlaXdWhpTYdd22BnL8z9pllvjlQUZ/vBthPaBWwy9FnP4A4VbfG3ii2fpE+ZXp8WLL6xCAJv7vjj2bzg/VUgxUS04vrdSXyKwE/iE6Fnl1x76/CM9S3SWGsEsM1UXFzeCNhZLfHJTdDupsqjiqeXL6fnQ0Otdjp8z3FjPtR+EvyqzsG52fxa1ZQmV+DwiZHygh/8AhtfqN0zKAysoEVL3VQJNewEVTmhCA4LuSAVOZwesd2TMcIQOGktEICmIMb3BjL5aiXMXpYniYRxpz3RNfnRMpJNlClnT4ywCcf7uAzUhFNa1C7YHQEfCbJx5dIvwPlWWGpoYBDR390GlFqyr3j9unIw6kdpd8e7TCdtYPCBjqdAYR+vofWiTSOgbouEZMlpI99e7Y1OI6Thvem9+cbCV2ypyuhE5AM9f19cZQIQcUfE/sPXywllcA1G2aM0AoCjd+HiGqS3HL5R4mfggbgpEQfQL+ITZJalagTHjGHS3YzQ0CYpsFxH2Nz2kUmHkRDHNLPpASBH0L6meEyr3OCK8QiKhNk7HJfUKxE1EanBi+H/fkF0Yy9i4lPs5J7LHk='
REGIAO = 'us-east-1'

BUCKET_RAW = 'bypass-trusted-1334'
BUCKET_TRUSTED = 'bypass-refined'

ARQUIVO_ENTRADA = 'train_consolidated_sla'
ARQUIVO_SAIDA = 'train_consolidated_sla_trusted'

con = S3Connector(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=REGIAO
)

spark = con.spark

s3_input_path = f"s3a://{BUCKET_RAW}/data/{ARQUIVO_ENTRADA}.csv"
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path, sep=",")

df_trusted = df_raw.filter(col("status").isNotNull()) \
                   .withColumnRenamed("sla", "sla_horas") \
                   .dropDuplicates()

s3_output_path = f"s3a://{BUCKET_TRUSTED}/data/{ARQUIVO_SAIDA}"
df_trusted.write.mode("overwrite").option("header", "true").csv(s3_output_path, sep=";")
