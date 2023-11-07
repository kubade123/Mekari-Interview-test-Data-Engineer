from datetime import date
import psycopg2
from db_config import DB_PARAMS

# Table name in the database, current_date untuk incremental
table_name = 'transformation.dedup_employees'
current_date = date.today()

# Membuat table apabila belum ada
def create_table():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
	employee_id int8 NULL,
	branch_id int8 NULL,
	salary numeric NULL,
	join_date date NULL,
	resign_date date NULL
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

# SQL query untuk dedup employees
join_query = f"""
TRUNCATE transformation.dedup_employees; 

INSERT INTO transformation.dedup_employees (employee_id, branch_id, salary, join_date, resign_date)
SELECT employee_id, branch_id, salary, join_date, resign_date
FROM (
    SELECT DISTINCT ON (employee_id, branch_id, join_date, resign_date)
        employee_id, branch_id, salary, join_date, resign_date,dt_created
    FROM staging.employees
    ORDER BY employee_id, branch_id, join_date, resign_date, salary DESC
) AS distinct_records
WHERE dt_created = '{current_date}';
"""

# Membuat koneksi ke DB
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

def perform_data_cleaning():
    cursor.execute(join_query)
    conn.commit()

if __name__ == '__main__':
    create_table()
    perform_data_cleaning()

    cursor.close()
    conn.close()