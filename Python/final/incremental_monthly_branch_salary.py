import psycopg2
from db_config import DB_PARAMS

# Table name in the database
table_name = 'staging.employees'


# SQL query untuk dedup employees
final_query = """
        INSERT INTO "final".monthly_branch_salary (year, month, branch_id, total_salary, unique_employee, total_work_hour, salary_per_hour)
        SELECT EXTRACT('YEAR' FROM et."date") as year,
            EXTRACT('MONTH' FROM et."date") as month,
            et.branch_id,
            sum(et.salary) as total_salary,
            count(distinct employee_id) as unique_employee,
            SUM(extract('HOUR' FROM et.checkout) - extract('HOUR' FROM et.checkin)) as total_work_hour,
            ROUND(sum(et.salary) / SUM(extract('HOUR' FROM et.checkout) - extract('HOUR' FROM et.checkin))) as salary_per_hour
        FROM transformation.employees_timesheets et
        -- WHERE et.branch_id = 1
        GROUP BY EXTRACT('YEAR' FROM et."date"), EXTRACT('MONTH' FROM et."date"), et.branch_id
        ORDER BY year, month, branch_id;
"""

# Connect to the PostgreSQL database
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()


def perform_data_cleaning():
    cursor.execute(final_query)
    conn.commit()

if __name__ == '__main__':
    perform_data_cleaning()

    cursor.close()
    conn.close()