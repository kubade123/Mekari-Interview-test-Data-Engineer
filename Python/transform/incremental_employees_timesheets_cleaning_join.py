from datetime import date
import psycopg2
from db_config import DB_PARAMS

# Table name in the database, current_date untuk incremental
current_date = date.today()
table_name = 'transformation.employees_timesheets'

# SQL query untuk join employee & timesheet serta cleaning
dedup_query = f"""
    DROP TABLE IF EXISTS {table_name};

    select de.employee_id,
    de.branch_id,
    de.salary,
    de.join_date ,
    de.resign_date ,
    t.timesheet_id ,
    t."date" ,
    t.checkin ,
    t.checkout 
    into transformation.employees_timesheets 
    from transformation.dedup_employees de 
    JOIN staging.timesheets t ON de.employee_id = t.employee_id
    WHERE t.checkin IS NOT NULL AND t.checkout IS NOT NULL
    AND t.timesheet_id NOT IN (
        SELECT timesheet_id
        FROM staging.timesheets
        WHERE t."date" > de.resign_date
    )
    AND t.timesheet_id NOT IN (
        SELECT timesheet_id
        FROM staging.timesheets
        WHERE t."date" < de.join_date
    )
    AND t.dt_created = '{current_date}'
    ORDER BY de.employee_id;
"""

# Membuat koneksi ke DB
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()


def perform_data_cleaning():
    cursor.execute(dedup_query)
    conn.commit()

if __name__ == '__main__':
    perform_data_cleaning()

    cursor.close()
    conn.close()