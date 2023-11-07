DROP TABLE IF EXISTS transformation.employees_timesheets;

CREATE TABLE transformation.employees_timesheets AS
SELECT de.employee_id, de.branch_id, de.salary, de.join_date, de.resign_date, t.timesheet_id, t."date", t.checkin, t.checkout
FROM transformation.dedup_employees de
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
ORDER BY de.employee_id;
