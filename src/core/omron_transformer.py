from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, col, mean, lit, when, count, sum as spark_sum
import os
import mysql.connector
from mysql.connector import Error
from transformer import tratar_dataframe

## Configuration
MYSQL_HOST = 'localhost'
MYSQL_DB = 'bypass_registry'
MYSQL_USER = 'bypass_user'
MYSQL_PASS = 'bypass1234'
TABLE_NAME = 'DADOS_TOF'

mysql_properties = {
    "driver": "com.mysql.cj.jdbc.Driver",
    "url": f"jdbc:mysql://{MYSQL_HOST}/{MYSQL_DB}",
    "user": MYSQL_USER,
    "password": MYSQL_PASS,
    "table": TABLE_NAME
}

os.environ['_JAVA_OPTIONS'] = '-Xmx1g'

spark = SparkSession.builder \
    .appName("Bypass-tranformer") \
    .getOrCreate()

path = "E:\\SPTech\\bypass\\bypass-tof\\data\\tof-sensor\\omron\\02-06-2025-01_29.json"

df = spark.read.json(path)
df = tratar_dataframe(df)

df.printSchema()

df.describe().show()

df.show(10, truncate=False)

# insert_into_registry(occupancy_df)

def associate_from_registry(df):
    """
    Associate ID_SENSOR with ID_PLATAFORMA
    """
    data = {}
    
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=3306
        )
        
        cursor = connection.cursor(dictionary=True)
        
        cursor.execute(f"SELECT ID_PLATAFORMA FROM PLATAFORMA WHERE ID_PLATAFORMA = (SELECT ID_PLATAFORMA FROM SENSOR WHERE ID_SENSOR = {df.first().asDict().get('sensor_id')});")

        records = cursor.fetchall()
        
        data = records[0] if records else {}
    except Error as e:
        print(f"MySQL Error: {str(e)}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    df = (df
            .withColumn("NUM_TREM", lit(data.get("NUM_TREM", 1)))
            .withColumn("NUM_CARRO", lit(data.get("NUM_CARRO", 1)))
            .drop("ID_SENSOR")
            .withColumnRenamed("y_block", "y")
            .withColumnRenamed("x_block", "x")
            .withColumnRenamed("timestamp", "DATAHORA")
            .select("y", "x", "dist", "DATAHORA", "NUM_TREM", "NUM_CARRO")
        )
    
    return df

