from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, dayofweek, dayofyear, col

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.env_config import db_properties

jdbc_url = f"jdbc:postgresql://{db_properties['db_host']}:{db_properties['db_port']}/{db_properties['db_name']}"

spark = SparkSession.builder \
    .appName("PostgreSQL-Spark Integration") \
    .config("spark.jars", db_properties['path_jdbc']) \
    .getOrCreate()

spark_properties = {
    "user": db_properties['db_user'],
    "password": db_properties['db_password'],
    "driver": "org.postgresql.Driver"
}

df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", db_properties['table_name']) \
    .options(**spark_properties) \
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

spark.stop()
