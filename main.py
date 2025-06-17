from src.core.s3_connector import S3Connector
from src.core.tof_transformer import TofTransformer
from src.core.dps_transformer import DpsTransformer
from src.core.piezo_transformer import PiezoTransformer
from src.core.transformer import Transformer

AWS_ACCESS_KEY_ID = 'ASIA2IC2FFOWAXFUFAHL'
AWS_SECRET_ACCESS_KEY = 'bZ07ZddX+bE+zPqOSSDJGpZkilXkvBd8wpbRIT/H'
AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEID//////////wEaCXVzLXdlc3QtMiJIMEYCIQCdo3C/fGP3eMgbh4v5o/3nmiFep4p7Qv8u/cEvhIrCJAIhAOmryIFWHn3lCpEZGwNPqaHqZIGVCK0AZAyiQ+F32dD5KrYCCGkQARoMNzA0NTYzNzE5MDg0Igx+zRn/biqUWF9KThkqkwInC1Y753spCa+duEIBJvAPC2zCT6RuXvcZY+K4tKLrDVqM8C1sGGnAgFzYMdyv9j3Nnbruae5SyAw5aek4/yGCTfSACVNX+Pi3FjlEPQNMd7J6hC3E3PTiSEJOceFtknHnUrpoxMAC9j6LYhLzGuFAItE6TiV/GaTgGkF1VTT9KNWrxGyVtSHzWKZORH6DGp0l0rHx3whtyKze7FuP4X5TOEcVGzxtDIBUBWRfq1koELHNp5ALZWey1Fuk1EKqwMWVN0LwBguTaeewyK15C62Y4JwlKoWoSLinrGF5FiHfvM5LZOm2vk1CWsy3wGQaP/1FgWVoXznUpYlRTEEmMb7uo+rs77RqixqMFYWQU3pHR5dLFDD53MLCBjqcAZdAHiW0qRse+Xr4/qn50lY5yr+hmGYW6rByrqH9tqctcYUnJToFgFxmZEyui38seT/UAtQQ22hDKpyoXxwH0wwVfrrWDqkKMxeKhHdXUgJIPV78L3poz1EXox4TW2fjOFDC6ciul8eVBKPld4hEQtte23tTM6aeNmEmEVso+/Jc/k/P5xhVCGTNPHHKwu3k0YmQklL+tZkY3/Pn8g=='
REGIAO = 'us-east-1'

BUCKET_RAW = 'bypass-teste-raw'
BUCKET_TRUSTED = 'bypass-teste-trusted'
BUCKET_CLIENT = 'bypass-teste-client'

# ARQUIVO = 'tof_sensor'
# ARQUIVO = 'dps_sensor'
ARQUIVO = 'piezo_sensor'

sensor_tabela = {
    "TOF": "DADOS_TOF",
    "DPS": "DADOS_DPS",
    "PIEZO": "DADOS_PIEZO"
}

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

print("-----------------DF_RAW-------------------")
df_raw.show(10)

transformer = Transformer(spark=spark, environment="docker")
print(transformer.db_con.url)

df_trusted = transformer.tratar_dataframe(df_raw)
print("-----------------DF_TRUSTED-------------------")
df_trusted.show(10)

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

sensor = str(df_tipo_sensor.first().asDict().get("TIPO_SENSOR")).strip()

if sensor == "TOF":
    transformer = TofTransformer(spark=spark, environment="docker")
    tabela = sensor_tabela.get("TOF")
elif sensor == "DPS":
    transformer = DpsTransformer(spark=spark, environment="docker")
    tabela = sensor_tabela.get("DPS")
elif sensor == "PIEZO":
    transformer = PiezoTransformer(spark=spark, environment="docker")
    tabela = sensor_tabela.get("PIEZO")

print("TIPO DO TRANSFORMER: ", type(transformer))

df_registry = transformer.tratar_dataframe_registry(df_trusted)

print("-----------------DF_REGISTRY-------------------")
df_registry.show(10)

transformer.insert_into_registry(
    df=df_registry,
    table_name=tabela
)

df_client = transformer.tratar_dataframe_client(df_trusted, spark)

print("-----------------DF_CLIENT-------------------")
df_client.show(10)

con.write_file_to_s3(
    df=df_client,
    sensor=sensor,
    bucket_name=BUCKET_CLIENT,
    file_name=ARQUIVO
)
