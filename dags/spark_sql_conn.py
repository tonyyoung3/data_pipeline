from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
import os

import sqlite3

def spark_conn():
    spark = SparkSession.builder.appName("SQLExample").getOrCreate()

    db_path= "./test_credit.db"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = "SELECT * FROM stock limit 10 "
    result = cursor.execute(query).fetchall()

    # Convert results to a list of Row objects
    row_objects = [Row(*row) for row in result]

    # Create a DataFrame from the Row objects
    df = spark.createDataFrame(row_objects)
    df_single_partition = df.coalesce(1)
    out_csv_file_path = os.path.dirname(__file__) + "/../output"
    df_single_partition.write.csv(out_csv_file_path, header=True, mode="overwrite")

    # Show the DataFrame
    # df.show()
    conn.close()
    spark.stop()
    
spark_conn()