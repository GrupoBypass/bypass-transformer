from s3_connector import S3Connector
from pyspark.sql.functions import col

AWS_ACCESS_KEY_ID = 'ASIA6L3Q6UCNBRKIONNX'
AWS_SECRET_ACCESS_KEY = 'md+NJXYx92b2UL6IFJtDF7+gsUrhOV0H0NAt3+x1'
AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEEkaCXVzLXdlc3QtMiJGMEQCIFP6Coj55KGW0WoL74nRXLhmCvtxLn1Onr9J/Bm6sjj7AiB46HStjZjJHkWxvtYeqVWSZYm2nx0iLjaCnD3Oe6EsNCq0AgghEAAaDDk4NzU0MjQ5NTM4NiIMrZgbMfrzT5QuTzokKpECSPYPBaufcdaK+rQVLMJDfQE2FkbZfsbfoadkdnXu4nqep94xVXvwpLKKd80ca/H+oWnSybRtXp9ju34jRMWGwXrgRAjW865MMJ8vJPUVtHcNLRZoyC8FwVCTSPOHaDOQY07bEkOYTX0y+XdhwFUurGalnOvmJqPBV7FzN6XQU3sHxj7KkoF7Emnxs/2ZH6jvZxtoPgmQh70NIw1mqBhgki5lV8a7jG/umrDEz9Jaw2qb2klHs3tjHBCPjpEkPTVFpLb9bpsqXg5MNmUvGgtPs2j1ZDiVsAbiXUk7vGuHBsRC9R6aLnof0WfrDadTw//Skero2hkVOP5TjdtZDJe0W/o2pYjdKOtD6rHOZg0LYcCCMOah/sEGOp4B6gSZcE6svJD7CCN3N/OFq9+OUioYL97HQGr9wHPLKqIAMtzpFIn0Hr17CTARou6DiwjNbWgUfAx/AsW5bzZ1qWoFyC7ryrhpaoGYdITNOVzGA+Pp/mvfj3kdnOKoh3QI3MrNyJ5wsZ/a76o1iP6UjjIUE2g7x30FyacWT/V360hc4HJDjXE3+Sm0X2wcU0w48pU8gSkP7tfZm9VSODA='
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
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path)

df_trusted = df_raw.filter(col("status").isNotNull()) \
                   .withColumnRenamed("sla", "sla_horas") \
                   .dropDuplicates()

s3_output_path = f"s3a://{BUCKET_TRUSTED}/data/{ARQUIVO_SAIDA}"
df_trusted.write.mode("overwrite").option("header", "true").csv(s3_output_path)
