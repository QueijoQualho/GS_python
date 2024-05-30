import pandas as pd
from db_functions import execute_query, create_table


def save_csv(csv_file: str, table_name: str) -> str:
    df = pd.read_csv(csv_file)

    df = df.where(pd.notna(df), None)

    columns = ", ".join(df.columns)
    placeholders = ", ".join([f":{i+1}" for i in range(len(df.columns))])
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    create_table(dataframe=df, table_name=table_name)

    return execute_query(insert_sql, data)
