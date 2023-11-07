import psycopg2
from psycopg2 import sql
from db_config import DB_PARAMS

# CSV file path
csv_file = 'D:\downloads\/timesheets.csv'

# Table name in the database
table_name = 'staging.timesheets'
columns = '(timesheet_id, employee_id, date, checkin, checkout)'

# Membuat koneksi ke DB
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

# Membuat table apabila belum ada, menambah kolom dt_created untuk incremental
def create_table():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
	timesheet_id int4 NULL,
	employee_id int4 NULL,
	"date" date NULL,
	checkin time NULL,
	checkout time NULL,
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
