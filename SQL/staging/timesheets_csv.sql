DO $$ 
DECLARE 
    file_path text := 'D:\downloads\timesheets.csv'; -- Atur file absolute path
BEGIN
    EXECUTE 'TRUNCATE timesheets;';

    -- csv to timesheets table
    EXECUTE 'COPY timesheets(timesheet_id, employee_id, date, checkin, checkout) FROM $1'
    USING file_path;
END $$;
