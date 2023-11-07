DROP TABLE IF EXISTS "final".monthly_branch_salary;

CREATE TABLE "final".monthly_branch_salary AS
SELECT
    EXTRACT('YEAR' FROM et."date") as year,
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
