import re

# Sessão Spark
from pyspark.sql import *

# Tipos de dados
from pyspark.sql.types import *

# Funções de DataFrame
from pyspark.sql.functions import *
from pyspark.sql.functions import round as spark_round

# Tipos de dados individuais (caso precise testar)
from pyspark.sql.types import *

class Transformer:
        
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
                # Pega amostra de até 20 valores não nulos
                amostra = (
                    df.select(nome_coluna)
                    .dropna()
                    .limit(20)
                    .rdd.map(lambda r: r[0])
                    .collect()
                )

                def parece_data(valor: str) -> bool:
                    """Verifica se a string bate com padrões comuns de data"""
                    if not isinstance(valor, str):
                        return False
                    valor = valor.strip()
                    padroes = [
                        r"^\d{4}-\d{2}-\d{2}$",                               # YYYY-MM-DD
                        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$",     # YYYY-MM-DD HH:MM:SS[.fração]
                        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\d*$",          # YYYY-MM-DD HH:MM:SSssss...
                        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?$",     # ISO 8601
                    ]
                    return any(re.match(p, valor) for p in padroes)

                total = len(amostra)
                datas = sum(1 for valor in amostra if parece_data(valor))

                # Converte para timestamp se ao menos 50% da amostra parecer data
                if total > 0 and datas / total >= 0.5:
                    print(f"Convertendo '{nome_coluna}' para TimestampType ({datas}/{total} amostras são datas)")
                    df = df.withColumn(
                        nome_coluna,
                        to_timestamp(col(nome_coluna), "yyyy-MM-dd HH:mm:ss")
                    )
                else:
                    print(f"Ignorando coluna '{nome_coluna}': não parece conter datas")

        print("Schema final:")
        df.printSchema()

        return df


