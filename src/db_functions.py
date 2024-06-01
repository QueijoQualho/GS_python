from connection.db_config import get_connection
from typing import List, Tuple, Optional
import pandas as pd
import oracledb
from pprint import pprint


def execute_query(sql: str, params: Optional[List[Tuple]] = None):
    connection = get_connection()
    cursor = connection.cursor()
    try:
        if params:
            cursor.executemany(sql, params)
        else:
            data_query: list[tuple] = list(cursor.execute(sql))
            return data_query

        connection.commit()

    except Exception as e:
        raise
    finally:
        cursor.close()
        connection.close()

def get_tab_column_names(table_name: str) -> list:
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT column_name 
            FROM all_tab_columns 
            WHERE table_name = '{table_name.upper()}'
            ORDER BY column_id
        """)
        colunas = cursor.fetchall()
        return [coluna[0] for coluna in colunas]
        

    except oracledb.DatabaseError as e:
        print(e)
    finally:
        cursor.close()
        connection.close()
        

def read_all_from_table(table_name: str):
    data_query: list[tuple] = execute_query(f"select * from {table_name}")

    return data_query

def read_by_entity(table_name: str, entity: str):
    try:
        data_query =  execute_query(f"select * from {table_name} where lower(entidade) like lower('%{entity}%')")
        column_names = get_tab_column_names(table_name=table_name) 
        
        dictionary_list: dict = sql_to_dict(column_names=column_names, data_query=data_query)

        print("--------------------------------------------\n\t\tRegistros")
        for dictionary in dictionary_list:
            format_and_print(dictionary)
        
    except oracledb.DatabaseError as e:
        (error, ) = e.args
        if error.code == 942:
            print(f"\nA tabela {table_name} não existe.\n")
        else:
            raise

def format_and_print(dictionary):
    formatted_string = "--------------------------------------------\n"
    for key, value in dictionary.items():
        formatted_string += f"    {key.capitalize()}: {value}\n"
    formatted_string += "--------------------------------------------"
    print(formatted_string)

def sql_to_dict(data_query: list[tuple],column_names:list) -> dict:
    dictionary_list = []

    for data in data_query:
            dictionary = {}
            for key, value in zip(column_names, data): 
                dictionary[key] = value
            dictionary_list.append(dictionary)
     
    return dictionary_list


    

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
    