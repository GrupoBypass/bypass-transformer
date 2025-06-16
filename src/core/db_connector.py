from pyspark.sql import DataFrame, SparkSession

class DbConnector:
    """Classe responsável para o acesso ao banco de dados."""
    
    def __init__(self, url: str, properties: str):
        self.url = url
        self.properties = properties

    def insert(self, df: DataFrame, table_name: str) -> None:
        """Método para inserir dados no banco de dados."""
        df.write.jdbc(url=self.url, table=table_name, mode="append", properties=self.properties)
        print(f"DataFrame escrito na tabela {table_name} com sucesso.")

    def select(self, spark: SparkSession, table_name: str) -> DataFrame:
        """Método para selecionar dados do banco de dados."""
        return spark.read.jdbc(url=self.url, table=table_name, properties=self.properties)
    
    def select_from_query(self, spark: SparkSession, query: str) -> DataFrame:
        """Método para executar uma consulta SQL e retornar um DataFrame."""
        return spark.sql(query)
    