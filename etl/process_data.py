from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, dayofweek, dayofyear, col

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.env_config import db_properties

# PostgreSQL JDBC URL
jdbc_url_postgres = f"jdbc:postgresql://{db_properties['db_host']}:{db_properties['db_port']}/{db_properties['db_name']}"

# Redshift JDBC URL
jdbc_url_redshift = f"jdbc:redshift://{db_properties['jdbc_redshift']}"


spark = SparkSession.builder \
    .appName("weather-data_Spark") \
    .config("spark.jars", f"{db_properties['path_postgres_jdbc']},{db_properties['path_redshift_jdbc']}") \
    .getOrCreate()

# Postgres connection properties
spark_properties_postgres = {
    "user": db_properties['db_user'],
    "password": db_properties['db_password'],
    "driver": "org.postgresql.Driver"
}

# Redshift connection properties
spark_properties_redshift = {
    "user": db_properties['redshift_user'],
    "password": db_properties['redshift_password'],
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

df = spark.read.format("jdbc") \
    .option("url", jdbc_url_postgres) \
    .option("dbtable", db_properties['table_name']) \
    .options(**spark_properties_postgres) \
    .load()

# Remove duplicates and missing values
df = df.dropDuplicates().dropna()

# Transfer to the right format
df = df.withColumn("date", to_date(col("datetime"), "yyyy-MM-dd"))

# Extract day of the year and day of the week
df = df.withColumn("day_of_week", dayofweek(col("date")))
df = df.withColumn("day_of_year", dayofyear(col("date")))

# Remove the datetime column
df = df.drop("datetime")

df.show()

df.write.format("jdbc") \
    .option("url", jdbc_url_redshift) \
    .option("dbtable", db_properties['table_name']) \
    .options(**spark_properties_redshift) \
    .mode("overwrite") \
    .save()

spark.stop()
