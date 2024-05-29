import oracledb
from dotenv import load_dotenv
import os

load_dotenv()

def get_connection():
    
    user = os.getenv('USER_BD')
    password= os.getenv('USER_PW')
    service_name = os.getenv("DSN")
    dsn_str = oracledb.makedsn("oracle.fiap.com.br",1521,service_name)
    connection = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn_str
    )
    return connection
