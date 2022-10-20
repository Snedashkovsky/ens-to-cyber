import pandas as pd

from config import bq_client, bqstorage_client


def get_df(query: str) -> pd.DataFrame:
    """
    Get Pandas DataFrame by SQL query
    :param query: SQL query to get data for a DataFrame
    :return: DataFrame
    """
    return bq_client.query(query).result().to_dataframe(bqstorage_client=bqstorage_client)
