import re

# Sessão Spark
from pyspark.sql import *

# Tipos de dados
from pyspark.sql.types import *

# Funções de DataFrame
from pyspark.sql.functions import col, coalesce, try_to_timestamp, from_utc_timestamp, date_format, round as spark_round

# Tipos de dados individuais (caso precise testar)
from pyspark.sql.types import *

import os

class Transformer:

    def tratar_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Trata um DataFrame PySpark padronizando valores:
        1. Remove registros com valores nulos
        2. Arredonda colunas do tipo Double para 2 casas decimais
        3. Converte colunas de data (Date, Timestamp ou strings com padrão de data) para formato 'yyyy-MM-dd HH:mm:ss'
        """

        # 1. Remove registros com valores nulos em QUALQUER coluna
        df = df.na.drop()

        # 2. Arredonda colunas Double para 2 casas decimais
        for field in df.schema.fields:
            if isinstance(field.dataType, DoubleType):
                df = df.withColumn(field.name, spark_round(col(field.name), 5))

        # 3. Converte colunas de data
        formatos = [
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "dd/MM/yyyy HH:mm:ss",
            "yyyy-MM-dd"
        ]

        for field in df.schema.fields:
            if isinstance(field.dataType, (DateType, TimestampType)):
                # Converte Date/Timestamp diretamente
                df = df.withColumn(
                    field.name,
                    date_format(col(field.name), "yyyy-MM-dd HH:mm:ss")
                )
            elif isinstance(field.dataType, StringType) and "date" in field.name.lower():
                # Tenta converter strings de data
                exprs = [try_to_timestamp(col(field.name), fmt) for fmt in formatos]
                df = df.withColumn(
                    field.name,
                    from_utc_timestamp(coalesce(*exprs), "America/Sao_Paulo")
                )
                df = df.withColumn(
                    field.name,
                    date_format(col(field.name), "yyyy-MM-dd HH:mm:ss")
                )

        return df

    def tratar_dataframe_registry(self, df: DataFrame) -> DataFrame:
        """
        Método de classe para tratar um DataFrame PySpark padronizando valores.
        
        Retorna:
            DataFrame: O DataFrame tratado
        """
        pass
    
    def tratar_dataframe_client(self, df: DataFrame) -> DataFrame:
        """
        Método de classe para tratar um DataFrame PySpark padronizando valores.
        
        Retorna:
            DataFrame: O DataFrame tratado
        """
        pass
    
