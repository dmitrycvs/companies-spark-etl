from dotenv import load_dotenv
import os

load_dotenv()

db_properties = {
    "db_name": os.getenv("DB_NAME"),
    "db_user": os.getenv("DB_USER"),
    "db_password": os.getenv("DB_PASSWORD"),
    "db_host": os.getenv("DB_HOST"),
    "db_port": os.getenv("DB_PORT"),
    "table_name": os.getenv("TABLE_NAME"),
    "path_jdbc": os.getenv("ABSOLUTE_PATH_TO_JDBC")
}