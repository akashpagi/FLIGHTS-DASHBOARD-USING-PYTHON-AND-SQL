import mysql.connector
from mysql.connector import Error

def create_database_and_table():
    try:
        # Connect to MySQL server to create database if it doesn't exist
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='root',
        )
        if conn.is_connected():
            mycursor = conn.cursor()
            print('Connection Established to MySQL Server!')

            # Create database if not exists
            mycursor.execute('CREATE DATABASE IF NOT EXISTS flights')
            print('Database created successfully or already exists.')
            mycursor.close()
            conn.close()

        # Connect to the 'flights' database to create table if it doesn't exist
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='flights',
        )
        if conn.is_connected():
            mycursor = conn.cursor()
            print('Connection Established to flights database!')

            # Create table if not exists
            mycursor.execute('''
                CREATE TABLE IF NOT EXISTS airport
                (
                    airport_id INT PRIMARY KEY,
                    code VARCHAR(10) NOT NULL,
                    city VARCHAR(50) NOT NULL,
                    name VARCHAR(255) NOT NULL
                )
            ''')
            print('Table created successfully or already exists.')
            
        # Step 3: Insert data into the 'airport' table   
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='flights',
            )
        if conn.is_connected():
            mycursor = conn.cursor()
            print('Connection Established to flights database for inserting data!')
            
            # Insert data into the 'airport' table using INSERT IGNORE to avoid reinsertions
            mycursor.execute('''
                INSERT IGNORE INTO airport (airport_id, code, city, name) VALUES
                (1, 'DEL', 'New Delhi', 'IGIA'),
                (2, 'CCU', 'Kolkata', 'NSCA'),
                (3, 'BOM', 'Mumbai', 'CSMA')
            ''')
            conn.commit()
            print('Records inserted successfully or already exist.')
             
    except Error as e:
        print(f'Connection Error: {e}')


def fetch_data():
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='flights',
            )
        if conn.is_connected():
            mycursor = conn.cursor()
            mycursor.execute('SELECT * FROM airport')
            # table = mycursor.fetchone() # fetchone() for single row
            table = mycursor.fetchall() 
            print(table) # output in the form of tuple
            
            # Printing the fetched data
            # for row in table:
            #     # print(row)
            #     print(row[3])

    except Error as e:
        print(f'Error: {e}')


def update_data():
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='flights',
            )
        if conn.is_connected():
            mycursor = conn.cursor()
            mycursor.execute('''
                             UPDATE airport 
                             SET name = 'T2' 
                             WHERE airport_id = 3
                             ''')
            conn.commit()
            print('Update successful !')
            fetch_data()

    except Error as e:
        print(f'Error: {e}')
        
        
def delete_data():
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='root',
            database='flights',
            )
        if conn.is_connected():
            mycursor = conn.cursor()
            mycursor.execute('''
                             DELETE FROM airport 
                             WHERE airport_id = 3
                             ''')
            conn.commit()
            print('ID Deleted successful !')
            fetch_data()

    except Error as e:
        print(f'Error: {e}')

# Function for create database and table, and insert initial data
create_database_and_table()

# Function for Fetch and print data from the 'airport' table
fetch_data()

update_data()

delete_data()