import sqlite3
import csv
import os

print(1)

def loading():
    print(1)
    conn = sqlite3.connect(os.path.dirname(__file__) +"/../test_credit.db")
    cursor = conn.cursor()
    csv_file_path = os.path.dirname(__file__) + "/../price_n.csv"
    print(1)
    with open(csv_file_path, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        # print(len(csv_reader))
        insert_query = "INSERT INTO stock (Date, Open, High, Low, Close, Volume, id) VALUES (?, ?, ?, ?, ?, ?, ?)"
        for row in csv_reader:
            cursor.execute(insert_query, row)
        conn.commit()

    # Display inserted data
    select_query = "SELECT * FROM stock"
    cursor.execute(select_query)
    results = cursor.fetchall()
    conn.close() 
    
loading()
    