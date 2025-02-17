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
    "path_postgres_jdbc": os.getenv("ABSOLUTE_PATH_TO_POSTGRES_JDBC"),
    "path_redshift_jdbc": os.getenv("ABSOLUTE_PATH_TO_REDSHIFT_JDBC"),
    "redshift_user": os.getenv("REDSHIFT_USER"),
    "redshift_password": os.getenv("REDSHIFT_PASSWORD"),
    "jdbc_redshift": os.getenv("JDBC_REDSHIFT")
}