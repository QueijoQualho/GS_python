import oracledb
from dotenv import load_dotenv
import os

load_dotenv()


def get_connection() -> oracledb.Connection:

    user = os.getenv("USER_BD")
    password = os.getenv("USER_PW")
    service_name = os.getenv("SERVICE_NAME")

    if not all([user, password, service_name]):
        raise ValueError("Alguma das variáveis de ambiente não está definida.")
    
    dsn_str = oracledb.makedsn("oracle.fiap.com.br", 1521, service_name)
    connection = oracledb.connect(user=user, password=password, dsn=dsn_str)
    return connection
