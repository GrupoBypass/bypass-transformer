from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from src.core.transformer import Transformer


class DHT11Transformer(Transformer):

    def __init__(self, spark: SparkSession, environment: str = "local"):
        super().__init__(spark=spark, environment=environment)

    def tratar_dataframe_registry(self, df: DataFrame) -> DataFrame:
        return df

    def tratar_dataframe_client(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        df_client = self.associar_trem_carro(spark, df)
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
              .select("DATAHORA", "TEMPERATURA", "UMIDADE", "NUM_TREM", "NUM_CARRO")
              )

        return df
