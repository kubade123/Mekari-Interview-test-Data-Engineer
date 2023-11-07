DROP TABLE IF EXISTS transformation.dedup_employees;

CREATE TABLE transformation.dedup_employees AS
SELECT employee_id, branch_id, salary, join_date, resign_date
FROM (
    SELECT DISTINCT ON (employee_id, branch_id, join_date, resign_date)
        employee_id, branch_id, salary, join_date, resign_date
    FROM staging.employees
    ORDER BY employee_id, branch_id, join_date, resign_date, salary DESC
) AS distinct_records;