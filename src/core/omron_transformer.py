from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, col, mean, lit, when, count, sum as spark_sum
from pyspark.sql.types import IntegerType
from src.core.transformer import Transformer


class OmronTransformer(Transformer):

    def __init__(self, spark: SparkSession, environment: str = "local"):
        super().__init__(spark=spark, environment=environment)
    
    def tratar_dataframe_registry(self, df: DataFrame) -> DataFrame:
        return df

    def insert_into_registry(self, df: DataFrame, table_name: str) -> None:
        """
        Sem implementação no registry ainda...
        
        Parâmetros:
            df (DataFrame): O DataFrame a ser escrito
            table_name (str): Nome da tabela MySQL
        """
        print("Sensor Infravermelho omron sem implementação no registry...")

    def tratar_dataframe_client(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        return df
    
    def associar_plataforma(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """
        Associar ID_SENSOR com ID_CARRO para pegar NUM_TREM e NUM_CARRO
        """

        df_plataforma = self.select_from_registry(spark=spark, table_name="PLATAFORMA")
        df_plataforma.createOrReplaceTempView("PLATAFORMA")

        df_sensor = self.select_from_registry(spark=spark, table_name="SENSOR")
        df_sensor.createOrReplaceTempView("SENSOR")

        df_id_plataforma = self.select_from_registry(
            spark=spark,
            query=f"SELECT ID_PLATAFORMA FROM PLATAFORMA WHERE ID_PLATAFORMA = (SELECT ID_PLATAFORMA FROM SENSOR WHERE ID_SENSOR = {df.first().asDict().get('sensor_id')});",
        )

        df = (df
              .withColumn("ID_PLATAFORMA", lit(df_id_plataforma.first().asDict().get("ID_PLATAFORMA", 1)))
              .drop("ID_SENSOR")
              .select("y", "x", "dist", "DATAHORA", "NUM_TREM", "NUM_CARRO")
              )

        return df
