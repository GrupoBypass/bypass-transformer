from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, col, mean, lit, when, count, sum as spark_sum
from pyspark.sql.types import IntegerType
from src.core.transformer import Transformer

class TofTransformer(Transformer):
    
    def __init__(self, environment: str = "local"):
        super().__init__(environment)

# path = "C:\\Users\\vitor\\Documents\\bypass\\bypass-tof\\data\\tof-sensor\\2025-06-04\\2025-06-04_2.csv"

# df = spark.read.option("header", True).option("inferSchema", True).csv(path)
# df = Transformer.tratar_dataframe(df)
    
    def tratar_dataframe_registry(self, df: DataFrame) -> DataFrame:
        return (df
            # Mark each measurement as occupied (1) or not (0)
            .withColumn("is_occupied",
                        when(col("dist_mm") < 1750, 1).otherwise(0))

            # Group by wagon and timestamp
            .groupBy("sensor_id", "timestamp")
            .agg(
                spark_sum("is_occupied").alias("occupied_points"),
                count(lit(1)).alias("total_points"),
                # Calculate percentage (rounded to 2 decimal places)
                (spark_sum("is_occupied") / count(lit(1)) * 100).alias("OCUPACAO_MEDIA")
            )
            .withColumnRenamed("sensor_id", "ID_SENSOR")
            .withColumnRenamed("timestamp", "DATAHORA")
            # Select and order columns
            .select(
                "ID_SENSOR",
                "DATAHORA",
                "OCUPACAO_MEDIA",
            )
        )

    def tratar_dataframe_client(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        
        df = (df
                .withColumn("y_block", floor(col("y") / 4).cast(IntegerType()))
                .withColumn("x_block", floor(col("x") / 4).cast(IntegerType()))
                .groupBy("sensor_id", "timestamp", "y_block", "x_block")
                .agg(
                    mean("dist_mm").alias("dist")
                )
                .select("y_block", "x_block", "dist", "timestamp", "sensor_id")
                .orderBy("y_block", "x_block")
            )
        
        df = self.associar_trem_carro(spark, df)
        return df

    def associar_trem_carro(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """
        Associar ID_SENSOR com ID_CARRO para pegar NUM_TREM e NUM_CARRO
        """
        
        df_composicao = self.select_from_registry(spark=spark, table="VW_COMPOSICAO_ATUAL")
        df_composicao.createOrReplaceTempView("VW_COMPOSICAO_ATUAL")

        df_sensor = self.select_from_registry(spark=spark, table="SENSOR")
        df_sensor.createOrReplaceTempView("SENSOR")

        df_trem_carro = self.select_from_registry(
            spark=spark,
            query=f"SELECT NUM_TREM, NUM_CARRO FROM VW_COMPOSICAO_ATUAL WHERE ID_CARRO = (SELECT ID_CARRO FROM SENSOR WHERE ID_SENSOR = {df.first().asDict().get('sensor_id')});",
        )

        df = (df
                .withColumn("NUM_TREM", lit(df_trem_carro.first().asDict().get("NUM_TREM", 1)))
                .withColumn("NUM_CARRO", lit(df_trem_carro.first().asDict().get("NUM_CARRO", 1)))
                .drop("ID_SENSOR")
                .withColumns("dist_mm", "dist")
                .withColumnRenamed("y_block", "y")
                .withColumnRenamed("x_block", "x")
                .withColumnRenamed("timestamp", "DATAHORA")
                .select("y", "x", "dist", "DATAHORA", "NUM_TREM", "NUM_CARRO")
            )
        
        return df
