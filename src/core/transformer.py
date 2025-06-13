import re

# Sessão Spark
from pyspark.sql import *

# Tipos de dados
from pyspark.sql.types import *

# Funções de DataFrame
from pyspark.sql.functions import col, coalesce, to_timestamp, round as spark_round

# Tipos de dados individuais (caso precise testar)
from pyspark.sql.types import *

class Transformer:
    
    @staticmethod
    def tratar_dataframe(df: DataFrame) -> DataFrame:
        """
        Trata um DataFrame PySpark padronizando valores:
        1. Remove registros com valores nulos
        2. Arredonda colunas do tipo Double para 2 casas decimais
        3. Converte colunas de data (Date, Timestamp ou strings com padrão de data) para formato 'yyyy-MM-dd HH:mm:ss'
        
        Parâmetros:
            df (DataFrame): O DataFrame de entrada

        Retorna:
            DataFrame: O DataFrame tratado
        """
        
        print("Schema original:")
        df.printSchema()

        # 1. Remove registros com qualquer valor nulo
        df = df.na.drop()

        # 2. Arredonda colunas do tipo DOUBLE
        for field in df.schema.fields:
            if isinstance(field.dataType, DoubleType):
                print(f"Arredondando coluna DOUBLE: {field.name}")
                df = df.withColumn(field.name, spark_round(col(field.name), 4))

        # 3. Converte datas (DateType, TimestampType ou strings que representem datas)
        for field in df.schema.fields:
            nome_coluna = field.name
            tipo_coluna = field.dataType

            # Caso a coluna já seja Date ou Timestamp
            if isinstance(tipo_coluna, (DateType, TimestampType)):
                print(f"Convertendo {nome_coluna} para Timestamp padronizado")
                df = df.withColumn(nome_coluna, col(nome_coluna).cast(TimestampType()))

            # Caso a coluna seja String, verificar se ela contém datas
            elif isinstance(tipo_coluna, StringType):
               # Check if string column contains dates
                date_patterns = [
                    r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$",         # dd/MM/yyyy HH:mm:ss
                    r"^\d{4}-\d{2}-\d{2}$",                           # YYYY-MM-DD
                    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",          # YYYY-MM-DD HH:MM:SS
                    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",          # ISO 8601
                    r"^\d{2}/\d{2}/\d{4}",                            # MM/DD/YYYY
                    r"^\d{2}-\d{2}-\d{4}"                             # DD-MM-YYYY
                ]
                
                sample = df.select(col(nome_coluna)).na.drop().limit(20).collect()
                date_count = sum(1 for row in sample if any(re.match(p, str(row[0]).strip()) for p in date_patterns))
                
                if len(sample) > 0 and date_count/len(sample) >= 0.5:
                    print(f"Converting '{nome_coluna}' to TimestampType ({date_count}/{len(sample)} samples look like dates)")
                    df = df.withColumn(
                        nome_coluna, 
                        coalesce(
                            to_timestamp(col(nome_coluna), "dd/MM/yyyy HH:mm:ss"),
                            to_timestamp(col(nome_coluna), "yyyy-MM-dd HH:mm:ss"),
                            to_timestamp(col(nome_coluna), "yyyy-MM-dd"),
                            to_timestamp(col(nome_coluna))
                        ),
                    )
                else:
                    print(f"Ignorando coluna '{nome_coluna}': não parece conter datas")

        print("Schema final:")
        df.printSchema()

        return df


