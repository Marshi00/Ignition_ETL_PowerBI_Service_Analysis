def log_script_run(path, start_time, finish_time, status, details="", startDate=None, endDate=None):
    query = """
        INSERT INTO script_run_info (path, start_time, finish_time, status, details, start_date, end_date)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    system.db.runPrepUpdate(query, [path, start_time, finish_time, status, details, sDate, eDate], "IQ_Report")
    print("Script run logged with start time:", start_time, "status:", status)

    