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

path = "E:\\SPTech\\bypass\\bypass-tof\\dps.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(path)
df = tratar_dataframe(df)

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
        query = f"""INSERT INTO DADOS_DPS
                   (ID_SENSOR, DATAHORA, `STATUS`, CORRENTE, TENSAO) 
                   VALUES (%s, %s, %s, %s, %s)"""
        
        # Convert to list of tuples
        data = [tuple(x) for x in pandas_df[['ID_SENSOR', 'DATAHORA', 'STATUS', 'CORRENTE', 'TENSAO']].values]
        
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

def associate_from_database(df):
    """
    Associate ID_SENSOR with ID_CIRCUITO, MODELO, CATEGORIA, PRIORIDADE, NUM_CARRO, NUM_TREM
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
        
        cursor.execute(f"""
                        SELECT 
                            C.ID_CIRCUITO
                            , C.MODELO
                            , C.CATEGORIA
                            , C.PRIORIDADE
                            , COMP.NUM_CARRO
                            , COMP.NUM_TREM 
                        FROM SENSOR S JOIN CIRCUITO C ON C.ID_CIRCUITO = S.ID_CIRCUITO JOIN VW_COMPOSICAO_ATUAL COMP ON COMP.ID_CARRO = C.ID_CARRO
                        WHERE ID_SENSOR = {df.first().asDict().get('ID_SENSOR')}
                       """)

        records = cursor.fetchall()
        
        data = records[0] if records else {}
    except Error as e:
        print(f"MySQL Error: {str(e)}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

    df_client = (df
            .withColumn("ID_CIRCUITO", lit(data.get("ID_CIRCUITO", 1)))
            .withColumn("MODELO", lit(data.get("MODELO", "Unknown")))
            .withColumn("CATEGORIA", lit(data.get("CATEGORIA", "Unknown")))
            .withColumn("PRIORIDADE", lit(data.get("PRIORIDADE", 1)))
            .withColumn("NUM_TREM", lit(data.get("NUM_TREM", 1)))
            .withColumn("NUM_CARRO", lit(data.get("NUM_CARRO", 1)))
            .select("ID_CIRCUITO", "MODELO", "CATEGORIA", "PRIORIDADE", "STATUS", "NUM_CARRO", "NUM_TREM")
        )
    
    return df_client

df_client = associate_from_database(df)

df_client.toPandas().to_csv('dps_20250531.csv', index=False, header=False)
