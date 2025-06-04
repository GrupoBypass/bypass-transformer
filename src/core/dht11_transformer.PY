from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import floor, col, mean, lit, when, count, sum as spark_sum
import numpy as np
import os
from urllib.request import urlopen
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

path = "E:\\SPTech\\bypass\\bypass-tof\\dht11.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(path)
df = tratar_dataframe(df)

df = (df
        .na.fill({"TEMPERATURA": 25.0})
        .na.fill({"UMIDADE": 50.0})
        .na.fill({"DATAHORA": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        .select('ID_SENSOR', 'DATAHORA', 'TEMPERATURA', 'UMIDADE')
    )

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
        query = f"""INSERT INTO DADOS_DHT11 
                   (ID_SENSOR, DATAHORA, TEMPERATURA, UMIDADE) 
                   VALUES (%s, %s, %s, %s)"""
        
        # Convert to list of tuples
        data = [tuple(x) for x in pandas_df[['ID_SENSOR', 'DATAHORA', 'TEMPERATURA', 'UMIDADE']].values]
        
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

insert_into_registry(df)
# FIM REGISTRY

def associate_trem_and_carro(df):
    """
    Associate ID_SENSOR with ID_CARRO to get NUM_TREM and NUM_CARRO
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
        
        cursor.execute(f"SELECT NUM_TREM, NUM_CARRO FROM VW_COMPOSICAO_ATUAL WHERE ID_CARRO = (SELECT ID_CARRO FROM SENSOR WHERE ID_SENSOR = {df.first().asDict().get('ID_SENSOR')})")

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
            .select("DATAHORA", "TEMPERATURA", "UMIDADE", "NUM_TREM", "NUM_CARRO")
        )
    
    return df

df_client = associate_trem_and_carro(df)

df_client.toPandas().to_csv('dht11_20250531.csv', index=False, header=False)
