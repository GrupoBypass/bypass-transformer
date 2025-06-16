from src.core.s3_connector import S3Connector
from src.core.tof_transformer import TofTransformer
from src.core.transformer import Transformer

AWS_ACCESS_KEY_ID = 'ASIA2IC2FFOWOIWGGT5F'
AWS_SECRET_ACCESS_KEY = 'rlX3HV7IRWpxVyV1gdSUeOJlwTv04VjA/IF1OhbQ'
AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEHsaCXVzLXdlc3QtMiJIMEYCIQDjxSGGLWqN91pBb4LYgUt/fSELtgEpF9yB2oJ2URU3vwIhAKoPQHoiwoGtrRe86BSz023rgAVytSWNt2xf3Ulw06fvKrYCCGQQARoMNzA0NTYzNzE5MDg0IgyK60aWnRt4sBUdqacqkwIreNkO0QS7RsIf9pNnSBnYbguW1zuJ6wSnXBFaCiDBhmRZPrQPCPczM7zbT1xHVsgWeTC63MnsQ8Na27ButZMkpAAUhOThAResRluagC0hx/G9UjMVilW0n7KsvueGKhajac86kfYzSz/5VSRsRJoHw6rsrJXSLnTBrQs7J52Cr7BDdhFfx2WEPav9kM24cNylc9G1JvMBFNLyKvC2uu8aDzSTG87ObwzHnE1YGbF6XbaPPfCjuF2eNSVVZl4M7o2315K+ZAyzNvmapjwK0p9+n9MBmXrTVONrfhfJemiJwtvIVUY6Gnzg9C+0/NVcJI/0WiYwMRc0EbtL+K5yQTIcjrsgV4eQ4VPrJ1EgYgJu0hxKFTDY1cHCBjqcAcDp+kx3cBYDkKusQfheH+97LGWkQOd/Gwr9RwctN80/NH3qMxbh17IQ0ZivAyRsu/fpVPkKokLV3jqPF1+hhD7OBnGyDMMIfObC7WD20b692XFocZD8/yuTTKPZ1NlcivsLZZg8nmUIiD8ac/iekVjqinwexcQHnAesvfLOwQ3F6dYbfVXA1NBvEAuTFdI3wGyROxI7Z7LcRQmFyQ=='
REGIAO = 'us-east-1'

BUCKET_RAW = 'bypass-teste-raw'
BUCKET_TRUSTED = 'bypass-teste-trusted'
BUCKET_CLIENT = 'bypass-teste-client'

ARQUIVO = 'tof_sensor'

con = S3Connector(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=REGIAO
)

spark = con.spark

df_raw = con.get_file_from_s3(
    file_name=ARQUIVO,
    bucket_name=BUCKET_RAW
)

df_raw.show(10)

transformer = Transformer(environment="docker")
print(transformer.db_con.url)

df_trusted = transformer.tratar_dataframe(df_raw)

con.write_file_to_s3(
    df=df_trusted,
    sensor=ARQUIVO,
    bucket_name=BUCKET_TRUSTED,
    file_name=ARQUIVO
)

df_sensor = transformer.select_from_registry(spark=spark, table_name="SENSOR")
df_sensor.createOrReplaceTempView("SENSOR")

df_tipo_sensor = transformer.select_from_registry(
    spark=spark,
    query=f"SELECT TIPO_SENSOR FROM SENSOR WHERE ID_SENSOR = {df_raw.first().asDict().get('sensor_id')};",
)

sensor = df_tipo_sensor.first().asDict().get("TIPO_SENSOR")

# Teste tof_tansformer
transformer = TofTransformer(environment="docker")

print("TIPO DO TRANSFORMER: ", type(transformer))

df_registry = transformer.tratar_dataframe_registry(df_trusted)

df_registry.show(10)

transformer.insert_into_registry(
    df=df_registry,
    table_name="DADOS_TOF"
)

df_client = transformer.tratar_dataframe_client(df_trusted, spark)

con.write_file_to_s3(
    df=df_client,
    sensor=sensor,
    bucket_name=BUCKET_CLIENT,
    file_name=ARQUIVO
)
