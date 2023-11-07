DO $$ 
DECLARE 
    file_path text := 'D:\downloads\employees.csv'; -- Atur file absolute path
BEGIN
    EXECUTE 'TRUNCATE employees;';

    -- csv to employees table
    EXECUTE 'COPY employees(employee_id, branch_id, salary, join_date, resign_date) FROM $1'
    USING file_path;
END $$;
