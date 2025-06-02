from core.transformer import Transformer
from core.s3_connector import S3Connector

def main():
    s3_raw = S3Connector()
    s3_trusted = S3Connector()
    s3_client = S3Connector()

    df = s3_raw.read_from_s3("data/dps/example_data.csv")

    transformer = Transformer()
    df_trusted = transformer.tratar_dataframe(df)

    s3_trusted.write_to_s3(df_trusted, "data/trusted/arquivo_trusted.csv")

    df_client = transformer.visao_dps(df_trusted)

    s3_client.write_to_s3(df_client, "data/client/arquivo_client.csv")

if __name__ == "__main__":
    main()