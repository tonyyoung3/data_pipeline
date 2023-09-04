import sqlite3
import csv
import os


def loading():

    conn = sqlite3.connect(os.path.dirname(__file__) +"/test_credit.db")
    cursor = conn.cursor()
    csv_file_path = os.path.dirname(__file__) + "/job_data/ontario.csv"

    with open(csv_file_path, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        # print(len(csv_reader))
        insert_query = "INSERT INTO job (role, company_name, location, post_date, description, level, job_type, job_func, industry, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        for row in csv_reader:
            cursor.execute(insert_query, row)
        conn.commit()

    # Display inserted data
    select_query = "SELECT * FROM job"
    cursor.execute(select_query)
    results = cursor.fetchall()
    print(results)
    conn.close() 
    
loading()