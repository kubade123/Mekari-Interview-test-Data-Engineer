import psycopg2
from psycopg2 import sql
from db_config import DB_PARAMS

# CSV file path
csv_file = 'D:\downloads\employees.csv'

# Table name in the database
table_name = 'staging.employees'
columns = '(employee_id, branch_id, salary, join_date, resign_date)'

# Membuat koneksi ke DB
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

# Membuat table apabila belum ada, menambah kolom dt_created untuk incremental
def create_table():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
	employee_id int8 NULL,
	branch_id int8 NULL,
	salary numeric NULL,
	join_date date NULL,
	resign_date date NULL,
	dt_created date NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

# Insert data dari csv ke table
def copy_data_from_csv():
    copy_query = sql.SQL(f"COPY {table_name}{columns} FROM %s WITH CSV HEADER DELIMITER ','")
    cursor.execute(copy_query, (csv_file,))
    conn.commit()

if __name__ == '__main__':
    create_table()
    copy_data_from_csv()

    cursor.close()
    conn.close()
