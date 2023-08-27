from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
import os

import sqlite3
import csv

def loading():
    conn = sqlite3.connect(os.path.dirname(__file__) + "/../test_credit.db")
    cursor = conn.cursor()
    csv_file_path = os.path.dirname(__file__) + "/../price_n.csv"
    with open(csv_file_path, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        insert_query = "INSERT INTO stock (Date, Open, High, Low, Close, Volume, id) VALUES (?, ?, ?, ?, ?, ?, ?)"
        for row in csv_reader:
            cursor.execute(insert_query, row)
        conn.commit()

    # Display inserted data
    select_query = "SELECT * FROM stock"
    cursor.execute(select_query)
    results = cursor.fetchall()
    conn.close() 


def spark_conn():
    spark = SparkSession.builder.appName("SQLExample").getOrCreate()

    db_path= "../test_credit.db"

    conn = sqlite3.connect(os.path.dirname(__file__) + "/../test_credit.db")
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
    #df.show()
    conn.close()
    spark.stop()    

def print_hello():
    return "Hello from the Python function!"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# loading()

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
)

start_task = DummyOperator(task_id='start', dag=dag)

python_task = PythonOperator(
    task_id='loading',
    python_callable=loading,
    dag=dag,
)

python_task_2 = PythonOperator(
    task_id='spark_conn',
    python_callable=spark_conn,
    dag=dag,
)



end_task = DummyOperator(task_id='end', dag=dag)

start_task >> python_task >> python_task_2 >> end_task
