from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    first as spark_first, 
    last as spark_last, 
    mean as spark_mean, 
    sum as spark_sum,
    max as spark_max,
    unix_timestamp,
    row_number,
    lit,
    col,
    udf,
    lag,
)
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
import os
from transformer import Transformer

os.environ['_JAVA_OPTIONS'] = '-Xmx2g'

# Set environment variable programmatically
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

jdbc_jar_path = "..\\bypass-transformer\\mysql-connector-j-9.3.0.jar"

spark = SparkSession.builder \
    .appName("bypass-tranformer") \
    .config("spark.jars", jdbc_jar_path) \
    .getOrCreate()

jdbc_url = "jdbc:mysql://localhost:3306/bypass_registry"
properties = {
    "user": "bypass_user",
    "password": "bypass1234",
    "driver": "com.mysql.cj.jdbc.Driver"
}

path = "C:\\Users\\vitor\\Documents\\bypass\\bypass-transformer\\data\\piezo_data.csv"

# spark.sparkContext.setLocalProperty("spark.python.program", ".\\.venv\\Scripts\\python.exe")

df = spark.read.option("header", True).option("inferSchema", True).csv(path, sep=';')
df = Transformer.tratar_dataframe(df)

df.show(10)

def registry_handler(df: DataFrame) -> DataFrame:
    """
    Handler para o DataFrame de registro, aplicando transformações específicas.
    """
    # Definir a janela para particionar por trem_id e ordenar por timestamp
    window_spec = Window.partitionBy("trem_id").orderBy("timestamp")

    # Adicionar um índice de linha dentro de cada partição
    df_with_row = df.withColumn("row_num", row_number().over(window_spec))

    # Filtrar para linhas pares (agrupando de 2 em 2)
    df_pairs = df_with_row.filter(col("row_num") % 2 == 0)

    # Juntar cada linha par com a linha anterior
    df_joined = df_pairs.alias("even").join(
        df_with_row.alias("odd"),
        (col("even.trem_id") == col("odd.trem_id")) & 
        (col("even.row_num") == col("odd.row_num") + 1),
        "inner"
    )

    if not 'VW_DISTANCIA_TRILHO' in [t.name for t in spark.catalog.listTables()]:
        df_distancias = spark.read.jdbc(url=jdbc_url, table="VW_DISTANCIA_TRILHO", properties=properties)
        df_distancias.createOrReplaceTempView("VW_DISTANCIA_TRILHO")

    # Calcular as métricas
    df_registry = df_joined.select(
        col("even.trem_id").alias("ID_TREM"),
        col("odd.sensor_id").alias("ID_SENSOR_ORIGEM"),
        col("even.sensor_id").alias("ID_SENSOR_DESTINO"),
        ((col("odd.pressure_kpa") + col("even.pressure_kpa")) / 2).alias("PRESSAO"),
        col("odd.timestamp").alias("DATAHORA_INICIO"),
        col("even.timestamp").alias("DATAHORA_FIM")
    )

    df_registry = df_registry.alias("R").join(
        spark.table("VW_DISTANCIA_TRILHO").alias("VW"),
        (col("R.ID_SENSOR_ORIGEM") == col("VW.SENSOR_1")) & 
        (col("R.ID_SENSOR_DESTINO") == col("VW.SENSOR_2")),
        "left"
    ) \
    .withColumn("TIMEDIFF", (unix_timestamp("DATAHORA_FIM") - unix_timestamp("DATAHORA_INICIO"))) \
    .withColumn("VELOCIDADE", col("DISTANCIA") / col("TIMEDIFF") * 3.6) \
    .drop("SENSOR_1", "SENSOR_2", "DISTANCIA", "TIMEDIFF") \
    .orderBy("DATAHORA_INICIO")

    df_registry.show()

    try:
        df_registry.write.jdbc(
            url=jdbc_url,
            table="DADOS_PIEZO",
            mode="append",
            properties=properties
        )
        print("Dados inseridos com sucesso!")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    
    return df_registry

df_client = registry_handler(df)

# Janela por sensor e ordenada por DATAHORA_INICIO
window_sensor = Window.partitionBy("ID_SENSOR_ORIGEM").orderBy("DATAHORA_INICIO")

# Calcula o timestamp de fim do trem anterior (no mesmo sensor)
df_client = df_client.withColumn(
    "DATAHORA_FIM_ANTERIOR",
    lag("DATAHORA_FIM").over(window_sensor)
)

# Calcula o headway em segundos
df_client = df_client.withColumn(
    "HEADWAY",
    unix_timestamp("DATAHORA_INICIO") - unix_timestamp("DATAHORA_FIM_ANTERIOR")
)

# Calcula o atraso (diferença do esperado de 180s)
df_client = df_client.withColumn(
    "ATRASO",
    col("HEADWAY") - lit(180)
)

# Exibe para conferência
df_client = df_client.drop("DATAHORA_FIM_ANTERIOR", "ID_TREM_ATRASO") \

df_client.show(10)



spark.stop()
