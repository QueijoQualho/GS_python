from connection.db_config import get_connection
from typing import List, Tuple, Optional
import pandas as pd
import oracledb


def execute_query(sql: str, params: Optional[List[Tuple]] = None):
    connection = get_connection()
    cursor = connection.cursor()
    try:
        if params:
            cursor.executemany(sql, params)
        else:
            data_query: list[tuple] = list(cursor.execute(sql))
            
            
            

        connection.commit()

    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()
        return data_query

def read_all_from_table(table_name: str):
    data_query: list[tuple] = execute_query(f"select * from {table_name}")

    return data_query


def map_pandas_type_to_oracle(pandas_type: str) -> str:
    if pandas_type == "int64":
        return "NUMBER"
    elif pandas_type == "float64":
        return "FLOAT"
    elif pandas_type == "bool":
        return "CHAR(1)"
    else:
        return "VARCHAR2(255)"


def create_table(dataframe: pd.DataFrame, table_name: str) -> None:
    connection = get_connection()
    oracle_types = [map_pandas_type_to_oracle(dtype.name) for dtype in dataframe.dtypes]

    columns_types_dict = dict(zip(dataframe.columns, oracle_types))

    column_definitions = ", ".join(
        [f"{col} {typ}" for col, typ in columns_types_dict.items()]
    )
    create_table_sql = f"CREATE TABLE {table_name} ({column_definitions})"

    cursor = connection.cursor()

    try:
        cursor.execute(f"DROP TABLE {table_name}")
    except oracledb.DatabaseError as e:
        (error,) = e.args
        if error.code == 942:  # Código de erro 942 indica que a tabela não existe
            pass
        else:
            raise

    cursor.execute(create_table_sql)
    cursor.close()
    connection.close()
