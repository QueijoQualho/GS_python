import pandas as pd
from connection.db_config import get_connection
from typing import List, Tuple

def read_csv(csv_file: str, table_name: str) -> str:
    df = pd.read_csv(csv_file)

    columns = ", ".join(df.columns)
    placeholders = ", ".join(["%s" for _ in df.columns])
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    data = [tuple(row) for row in df.itertuples(index=False, name=None)]
    
    return execute_query(insert_sql, data)
    
def execute_query(sql: str, params: List[Tuple]) -> str:
    connection = get_connection()
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, params)
        connection.commit()
        return "Query executed successfully."
    except Exception as e:
        return str(e)
    finally:
        cursor.close()
        connection.close()
    
