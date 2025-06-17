from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from src.core.transformer import Transformer

class DpsTransformer(Transformer):

    def __init__(self, environment: str = "local"):
        super().__init__(environment=environment)

    def tratar_dataframe_registry(self, df: DataFrame) -> DataFrame:
        return (df
                .withColumnRenamed("dataHora", "DATAHORA")
                .withColumnRenamed("statusDPS", "STATUS")
                .withColumnRenamed("picoTensao_kV", "TENSAO")
                .withColumnRenamed("correnteSurto_kA", "CORRENTE")
                .withColumnRenamed("sensor_id", "ID_SENSOR")
        )

    def tratar_dataframe_client(self, df: DataFrame, spark: SparkSession) -> DataFrame:

        df_composicao = self.select_from_registry(spark=spark, table_name="VW_COMPOSICAO_ATUAL")
        df_composicao.createOrReplaceTempView("VW_COMPOSICAO_ATUAL")

        df_sensor = self.select_from_registry(spark=spark, table_name="SENSOR")
        df_sensor.createOrReplaceTempView("SENSOR")
        
        df_circuito = self.select_from_registry(spark=spark, table_name="CIRCUITO")
        df_circuito.createOrReplaceTempView("CIRCUITO")

        df_dps = self.select_from_registry(
            spark=spark,
            query=f"""
                    SELECT 
                        C.ID_CIRCUITO
                        , C.MODELO
                        , C.CATEGORIA
                        , C.PRIORIDADE
                        , COMP.NUM_CARRO
                        , COMP.NUM_TREM 
                    FROM SENSOR S JOIN CIRCUITO C ON C.ID_CIRCUITO = S.ID_CIRCUITO JOIN VW_COMPOSICAO_ATUAL COMP ON COMP.ID_CARRO = C.ID_CARRO
                    WHERE ID_SENSOR = {df.first().asDict().get('sensor_id')}
                """
        )

        return (df
            .withColumn("ID_CIRCUITO", lit(df_dps.first().asDict().get('ID_CIRCUITO', 1)))
            .withColumn("MODELO", lit(df_dps.first().asDict().get('MODELO', "Unknown")))
            .withColumn("CATEGORIA", lit(df_dps.first().asDict().get('CATEGORIA', "Unknown")))
            .withColumn("PRIORIDADE", lit(df_dps.first().asDict().get('PRIORIDADE', 1)))
            .withColumn("NUM_TREM", lit(df_dps.first().asDict().get('NUM_TREM', 1)))
            .withColumn("NUM_CARRO", lit(df_dps.first().asDict().get('NUM_CARRO', 1)))
            .withColumnRenamed("statusDPS", "STATUS")
            .withColumnRenamed("dataHora", "DATAHORA")
            .select("ID_CIRCUITO", "MODELO", "CATEGORIA", "PRIORIDADE", "STATUS", "NUM_CARRO", "NUM_TREM", "DATAHORA")
        )

