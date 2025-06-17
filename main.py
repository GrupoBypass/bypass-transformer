from src.core.s3_connector import S3Connector
from src.core.tof_transformer import TofTransformer
from src.core.dps_transformer import DpsTransformer
from src.core.piezo_transformer import PiezoTransformer
from src.core.omron_transformer import OmronTransformer
from src.core.dht11_transformer import DHT11Transformer
from src.core.transformer import Transformer

AWS_ACCESS_KEY_ID = 'ASIA2IC2FFOWFLT53EO4'
AWS_SECRET_ACCESS_KEY = 'b3h0oyjNfIPZVIs2hQm4FAux36eSASm3x71nDwzf'
AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEJX//////////wEaCXVzLXdlc3QtMiJHMEUCIQCuBio/tb7tjJBFTy67FybNcyZBXgMQEEQyeu1y6uh5rwIgH1eGhiPkq0DWueJxO81GgVT6dNwfqRbg23YstlTnV7sqtgIIfhABGgw3MDQ1NjM3MTkwODQiDL5GlrsqIUr6jkG4+iqTAvYgBgvgeivkxlXRQWzwdRwZFLGlzuBa8aOsTuzUEcmasf04qJ7JVw08+kcns4Ygn2p/CKMSv+DxDyT8iQthjIldy0bwNJ+ZSYnak7vyfhSMVtoveivTSenR0ka5ELhdBsqjE+nq5VPWQ4x7AV3tcJWujh/WVZApK4PlLEiGE1QpAvRxgL+2vQCDzgENXNDlLTTEwfwmp7m1ECBBm6812URQqJ5IcHIjmvwHF6NTrokPt5y23/SXhpkkL7hkC24gd/FgLgA7LAhLbjsCG2cuMqxXbJfaVSJUJ+xyrKM2szfrVk7wWzidPklPZ65nwbkVZoalL0aXUM8HQvzUDFUQjtVC1ZN1z7no/39ymJDtCkTqIH8oMLOyx8IGOp0B+1sOh1Ev4kBXat4R8sC2dh6kR8eQx24lnbIVsfZLH2jm3VwBdJJFMtZwWu7GqfNO/K5cyupGAzrrxwApi0LTlWcO0hrIP/oUt/ISdctTZYXUIYdArQyzz3Hv9PVg+I6J8d7rJdeyQce89OLiL2tORetWdN6vJanIolB6YynBxTi9yIRszPY4CRotjL7OE98ruX6faHUI7hlaeUwQEg=='
REGIAO = 'us-east-1'

BUCKET_RAW = 'bypass-teste-raw'
BUCKET_TRUSTED = 'bypass-teste-trusted'
BUCKET_CLIENT = 'bypass-teste-client'

# ARQUIVO = 'tof_sensor'
# ARQUIVO = 'dps_sensor'
ARQUIVO = 'piezo_sensor'
# ARQUIVO = 'omron_sensor'

sensor_tabela = {
    "TOF": "DADOS_TOF",
    "DPS": "DADOS_DPS",
    "PIEZO": "DADOS_PIEZO",
    "OMRON": "DADOS_OMRON",
    "DHT11": "DADOS_DHT11",
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
elif sensor == "OMRON":
    transformer = OmronTransformer(spark=spark, environment="docker")
    tabela = sensor_tabela.get("OMRON")
elif sensor == "UMIDADE/TEMPERATURA":
    sensor = "DHT11"
    transformer = DHT11Transformer(spark=spark, environment="docker")
    tabela = sensor_tabela.get("DHT11")

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
