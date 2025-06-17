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
from pyspark.sql.window import Window
from src.core.transformer import Transformer

class PiezoTransformer(Transformer):

    def __init__(self, spark: SparkSession, environment: str = "local"):
        super().__init__(spark=spark, environment=environment)

    def tratar_dataframe_registry(self, df: DataFrame) -> DataFrame:
        """
        Handler para o DataFrame de registro, aplicando transformações específicas.
        """
        # Definir a janela para particionar por trem_id e ordenar por timestamp
        window_spec = Window.partitionBy("trem_id").orderBy("dataHora")

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

        if not 'VW_DISTANCIA_TRILHO' in [t.name for t in self.spark.catalog.listTables()]:
            df_distancias = self.select_from_registry(spark=self.spark, table_name="VW_DISTANCIA_TRILHO")
            df_distancias.createOrReplaceTempView("VW_DISTANCIA_TRILHO")

        # Calcular as métricas
        df_registry = df_joined.select(
            col("even.trem_id").alias("ID_TREM"),
            col("odd.sensor_id").alias("ID_SENSOR_ORIGEM"),
            col("even.sensor_id").alias("ID_SENSOR_DESTINO"),
            ((col("odd.pressure_kpa") + col("even.pressure_kpa")) / 2).alias("PRESSAO"),
            col("odd.dataHora").alias("DATAHORA_INICIO"),
            col("even.dataHora").alias("DATAHORA_FIM")
        )

        df_registry = df_registry.alias("R").join(
            self.spark.table("VW_DISTANCIA_TRILHO").alias("VW"),
            (col("R.ID_SENSOR_ORIGEM") == col("VW.SENSOR_1")) & 
            (col("R.ID_SENSOR_DESTINO") == col("VW.SENSOR_2")),
            "left"
        ) \
        .withColumn("TIMEDIFF", (unix_timestamp("DATAHORA_FIM") - unix_timestamp("DATAHORA_INICIO"))) \
        .withColumn("VELOCIDADE", col("DISTANCIA") / col("TIMEDIFF") * 3.6) \
        .drop("SENSOR_1", "SENSOR_2", "DISTANCIA", "TIMEDIFF") \
        .orderBy("DATAHORA_INICIO")

        df_registry = df_registry.filter(col("ID_SENSOR_ORIGEM") != col("ID_SENSOR_DESTINO"))
        return df_registry

    def tratar_dataframe_client(self, df: DataFrame, spark: SparkSession) -> DataFrame:

        df_client = self.tratar_dataframe_registry(df)

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
        df_client = df_client.drop("DATAHORA_FIM_ANTERIOR", "ID_TREM_ATRASO")
        
        return df_client

    def associar_trem_carro(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """
        Associar ID_SENSOR com ID_CARRO para pegar NUM_TREM e NUM_CARRO
        """

        df_composicao = self.select_from_registry(
            spark=spark, table_name="VW_COMPOSICAO_ATUAL")
        df_composicao.createOrReplaceTempView("VW_COMPOSICAO_ATUAL")

        df_sensor = self.select_from_registry(spark=spark, table_name="SENSOR")
        df_sensor.createOrReplaceTempView("SENSOR")

        df_trem_carro = self.select_from_registry(
            spark=spark,
            query=f"SELECT NUM_TREM, NUM_CARRO FROM VW_COMPOSICAO_ATUAL WHERE ID_CARRO = (SELECT ID_CARRO FROM SENSOR WHERE ID_SENSOR = {df.first().asDict().get('sensor_id')});",
        )

        df = (df
              .withColumn("NUM_TREM", lit(df_trem_carro.first().asDict().get("NUM_TREM", 1)))
              .withColumn("NUM_CARRO", lit(df_trem_carro.first().asDict().get("NUM_CARRO", 1)))
              .drop("ID_SENSOR")
              .withColumnRenamed("dist_mm", "dist")
              .withColumnRenamed("y_block", "y")
              .withColumnRenamed("x_block", "x")
              .withColumnRenamed("dataHora", "DATAHORA")
              .select("y", "x", "dist", "DATAHORA", "NUM_TREM", "NUM_CARRO")
              )

        return df
