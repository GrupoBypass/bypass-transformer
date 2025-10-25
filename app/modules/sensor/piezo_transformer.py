from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    row_number,
    unix_timestamp,
    lag,
    lit,
    regexp_replace
)
from pyspark.sql.window import Window
from modules.cleaning.transformer import Transformer
import os
import boto3
from decimal import Decimal

class PiezoTransformer(Transformer):
    
    def main(self, local_input, s3, key):
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_trusted = "bucket-bypass-trusted-teste"
        
        print("Iniciando Spark...")
        spark = SparkSession.builder.appName("PiezoSpark").getOrCreate()
        
        print(f"Lendo CSV: {local_input}")
        df = spark.read.option("header", True).option("inferSchema", "true").csv(local_input)
        print(f"Linhas lidas: {df.count()}")
        
        df = super().tratar_dataframe(df)
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(local_output_dir)

        # Renomeia o arquivo final para resultado.csv
        for file_name in os.listdir(local_output_dir):
            if file_name.endswith(".csv"):
                src_path = os.path.join(local_output_dir, file_name)
                os.rename(src_path, local_output_file)
                print(f"Arquivo final gerado: {local_output_file}")

        # Envia arquivo para o S3
        s3.upload_file(local_output_file, bucket_trusted, key)
        print(f"Arquivo enviado para: s3://{bucket_trusted}/{key}")
        
        df = self.tratar_dataframe_registry(df, spark)
        
        self.tratar_dataframe_client(df, s3, key)
        

    def convert_decimals(self, obj):
        if isinstance(obj, list):
            return [self.convert_decimals(i) for i in obj]
        elif isinstance(obj, dict):
            return {k: self.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            return float(obj)
        else:
            return obj

    def tratar_dataframe_registry(self, df: DataFrame, spark) -> DataFrame:
        """
        Handler para o DataFrame de registro, aplicando transformações específicas.
        """
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        piezo_table = dynamodb.Table("PiezoData")
        piezo_sensor_distancia = dynamodb.Table("PiezoSensorDistancia")

        # 1. Definir janela para particionar por trem_id e ordenar por timestamp
        window_spec = Window.partitionBy("trem_id").orderBy("dataHora")

        # 2. Adicionar índice de linha dentro de cada partição
        df_with_row = df.withColumn("row_num", row_number().over(window_spec))

        # 3. Filtrar para linhas pares (agrupando de 2 em 2)
        df_pairs = df_with_row.filter(col("row_num") % 2 == 0)

        # 4. Juntar cada linha par com a anterior (linha ímpar)
        df_joined = df_pairs.alias("even").join(
            df_with_row.alias("odd"),
            (col("even.trem_id") == col("odd.trem_id")) &
            (col("even.row_num") == col("odd.row_num") + 1),
            "inner"
        )

        # 5. Calcular métricas iniciais
        df_registry = df_joined.select(
            col("even.trem_id").alias("ID_TREM"),
            col("odd.sensor_id").alias("ID_SENSOR_ORIGEM"),
            col("even.sensor_id").alias("ID_SENSOR_DESTINO"),
            ((col("odd.pressure_kpa") + col("even.pressure_kpa")) / 2).alias("PRESSAO"),
            col("odd.dataHora").alias("DATAHORA_INICIO"),
            col("even.dataHora").alias("DATAHORA_FIM")
        )

        # 6. Carregar distâncias do DynamoDB
        response = piezo_sensor_distancia.scan()
        items = response["Items"]

        # Continuar scan se houver paginação
        while "LastEvaluatedKey" in response:
            response = piezo_sensor_distancia.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response["Items"])
            
        items = [self.convert_decimals(item) for item in items]
        
        df_distancia = spark.createDataFrame(items)
        
        # Criar DataFrame invertido
        df_invertido = df_distancia.select(
            col("sensor_id_2").alias("sensor_id"),
            col("sensor_id").alias("sensor_id_2"),
            col("distancia")
        )

        # Unir com o original
        df_distancia = df_distancia.unionByName(df_invertido)

        # no seu DataFrame do CSV
        df_distancia = df_distancia.withColumn(
            "sensor_id",
            regexp_replace(col("sensor_id").cast("string"), "^(S)", "")
        ).withColumn(
            "sensor_id_2",
            regexp_replace(col("sensor_id_2").cast("string"), "^(S)", "")
        )

        # 7. Join com tabela de distâncias
        df_registry = df_registry.alias("R").join(
            df_distancia.alias("D"),
            (col("R.ID_SENSOR_ORIGEM") == col("D.sensor_id")) &
            (col("R.ID_SENSOR_DESTINO") == col("D.sensor_id_2")),
            "left"
        ).withColumn(
            "TIMEDIFF",
            unix_timestamp("DATAHORA_FIM") - unix_timestamp("DATAHORA_INICIO")
        ).withColumn(
            "VELOCIDADE",
            (col("D.distancia") / col("TIMEDIFF")) * 3.6
        ).drop("sensor_id", "sensor_id_2", "distancia", "TIMEDIFF") \
        .orderBy("DATAHORA_INICIO")

        # 8. Garantir que não tenha sensores iguais
        df_registry = df_registry.filter(col("ID_SENSOR_ORIGEM") != col("ID_SENSOR_DESTINO"))
        
        # Insere no DynamoDB (tabela TofData)
        for row in df_registry.collect():
            sensor_origem_value = row.get("ID_SENSOR_ORIGEM")
            sensor_id_origem = f"S{sensor_origem_value}"
            sensor_destino_value = row.get("ID_SENSOR_DESTINO")
            sensor_id_destino = f"S{sensor_destino_value}"
            trem_value = row.get("ID_TREM")
            trem_id = f"T{trem_value}"
            pressao = float(row["PRESSAO"])
            velocidade = float(row["VELOCIDADE"])
            datahora_inicio = row["DATAHORA_INICIO"]
            datahora_fim = row["DATAHORA_FIM"]

            # Insere no DynamoDB (tabela TofData)
            piezo_table.put_item(
                Item={
                    "trem_id": trem_id,
                    "sensor_id_origem": sensor_id_origem,
                    "sensor_id_destino": sensor_id_destino,
                    "pressao": Decimal(str(pressao)),
                    "velocidade": Decimal(str(velocidade)),
                    "datahora_inicio": datahora_inicio,
                    "datahora_fim": datahora_fim
                }
            )
            print(f"✅ Inserido PiezoData: {trem_id} com velocidade de {velocidade} km/h")

        return df_registry

    def tratar_dataframe_client(self, df: DataFrame, s3, key) -> DataFrame:
        """
        Pipeline completo: aplica registry e associa linha/trilho.
        """
        local_output_dir = "/tmp/output"
        local_output_file = "/tmp/resultado.csv"   # nome fixo
        bucket_client = "bucket-bypass-client-teste"

        # Janela por sensor e ordenada por DATAHORA_INICIO
        window_sensor = Window.partitionBy("ID_SENSOR_ORIGEM").orderBy("DATAHORA_INICIO")

        # Calcula headway em segundos
        df = df.withColumn(
            "DATAHORA_FIM_ANTERIOR",
            lag("DATAHORA_FIM").over(window_sensor)
        ).withColumn(
            "HEADWAY",
            unix_timestamp("DATAHORA_INICIO") - unix_timestamp("DATAHORA_FIM_ANTERIOR")
        )

        # Limpeza de colunas auxiliares
        df = df.drop("DATAHORA_FIM_ANTERIOR", "ID_TREM_ATRASO")
        
        

        df.coalesce(1).write.mode("overwrite").option("header", True).csv(local_output_dir)

        # Renomeia o arquivo final para resultado.csv
        for file_name in os.listdir(local_output_dir):
            if file_name.endswith(".csv"):
                src_path = os.path.join(local_output_dir, file_name)
                os.rename(src_path, local_output_file)
                print(f"Arquivo final gerado: {local_output_file}")

        # Envia arquivo para o S3
        s3.upload_file(local_output_file, bucket_client, key)
        print(f"Arquivo enviado para: s3://{bucket_client}/{key}")

    def associar_trilho_linha(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        """
        Associa ID_SENSOR com TRILHO/LINHA.
        Mantive o método igual, mas você também pode mover TRILHO/LINHA para DynamoDB depois.
        """
        df_trilho = self.select_from_registry(spark=spark, table_name="TRILHO")
        df_trilho.createOrReplaceTempView("TRILHO")

        df_sensor = self.select_from_registry(spark=spark, table_name="SENSOR")
        df_sensor.createOrReplaceTempView("SENSOR")

        df_linha = self.select_from_registry(spark=spark, table_name="LINHA")
        df_linha.createOrReplaceTempView("LINHA")

        df_trilho_linha = self.select_from_registry(
            spark=spark,
            query=f"""
                SELECT T.NUM_IDENTIFICACAO AS TRILHO,
                       CONCAT(L.NUMERO, " - ", L.COR_IDENTIFICACAO) AS LINHA
                FROM TRILHO T 
                JOIN LINHA L ON L.ID_LINHA = T.ID_LINHA
                WHERE T.ID_TRILHO = (
                    SELECT ID_TRILHO FROM SENSOR WHERE ID_SENSOR = {df.first().asDict().get('ID_SENSOR_ORIGEM')}
                );
            """,
        )

        df = (df
              .withColumn("TRILHO", lit(df_trilho_linha.first().asDict().get("TRILHO", 1)))
              .withColumn("LINHA", lit(df_trilho_linha.first().asDict().get("LINHA", 1)))
              )

        return df
