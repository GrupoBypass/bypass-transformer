from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, col, mean, lit, when, count, sum as spark_sum
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os
import mysql.connector
from mysql.connector import Error
from transformer import Transformer

os.environ['_JAVA_OPTIONS'] = '-Xmx1g'

# Set environment variable programmatically
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

jdbc_jar_path = "E:\\SPTech\\bypass\\bypass-transformer\\mysql-connector-j-9.3.0.jar"

spark = SparkSession.builder \
    .appName("bypass-tranformer") \
    .config("spark.jars", jdbc_jar_path) \
    .getOrCreate()

url = "jdbc:mysql://localhost:3306/bypass_registry"
properties = {
    "user": "bypass_user",
    "password": "bypass1234",
    "driver": "com.mysql.cj.jdbc.Driver"
}

path = "E:\\SPTech\\bypass\\bypass-tof\\data\\tof-sensor\\2025-06-03\\2025-06-03_2.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(path)
df = Transformer.tratar_dataframe(df)

df.printSchema()

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

df.show(10, truncate=False)

occupancy_df = (df
    # Mark each measurement as occupied (1) or not (0)
    .withColumn("is_occupied",
                when(col("dist") < 1750, 1).otherwise(0))

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

occupancy_df.show(10, truncate=False)

def insert_into_registry(df):
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=3306
        )
        
        cursor = connection.cursor()
        
        # Convert Spark DataFrame to Pandas (for small datasets)
        pandas_df = df.toPandas()
        
        # Prepare insert query
        query = f"""INSERT INTO {TABLE_NAME} 
                   (ID_SENSOR, DATAHORA, OCUPACAO_MEDIA) 
                   VALUES (%s, %s, %s)"""
        
        # Convert to list of tuples
        data = [tuple(x) for x in pandas_df[['ID_SENSOR', 'DATAHORA', 'OCUPACAO_MEDIA']].values]
        
        # Execute batch insert
        cursor.executemany(query, data)
        connection.commit()
        
        print(f"Inserted {len(pandas_df)} records successfully")
        
    except Error as e:
        print(f"MySQL Error: {str(e)}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# insert_into_registry(occupancy_df)

def associate_trem_and_carro(df):
    """
    Associate ID_SENSOR with ID_CARRO to get NUM_TREM and NUM_CARRO
    """
    
    try:
        df_composicao = spark.read.jdbc(url=url, table="VW_COMPOSICAO_ATUAL", properties=properties)
        df_composicao.show()
        df_composicao.printSchema()
        df_composicao.createOrReplaceTempView("VW_COMPOSICAO_ATUAL")

        df_sensor = spark.read.jdbc(url=url, table="SENSOR", properties=properties)
        df_sensor.show()
        df_sensor.printSchema()
        df_sensor.createOrReplaceTempView("SENSOR")

        df_composicao = spark.sql(f"SELECT NUM_TREM, NUM_CARRO FROM VW_COMPOSICAO_ATUAL WHERE ID_CARRO = (SELECT ID_CARRO FROM SENSOR WHERE ID_SENSOR = {df.first().asDict().get('sensor_id')});")
        df_composicao.show()
        df_composicao.printSchema()

    except Exception as e:
        print("Erro ao conectar ou ler do MySQL:")
        print(e)

    df = (df
            .withColumn("NUM_TREM", lit(df_composicao.first().asDict().get("NUM_TREM", 1)))
            .withColumn("NUM_CARRO", lit(df_composicao.first().asDict().get("NUM_CARRO", 1)))
            .drop("ID_SENSOR")
            .withColumnRenamed("y_block", "y")
            .withColumnRenamed("x_block", "x")
            .withColumnRenamed("timestamp", "DATAHORA")
            .select("y", "x", "dist", "DATAHORA", "NUM_TREM", "NUM_CARRO")
        )
    
    return df

df_client = associate_trem_and_carro(df)

df_client.show(10, truncate=False)

spark.stop()
