def upsert_db_populate_from_pydataset(pyDataSet, dbConn, batchSize, customPath):
    rows = pyDataSet.getRowCount()
    query = """
        INSERT INTO records (T_stamp, ignition_path, value)
        VALUES (?, ?, ?)
        ON CONFLICT (T_stamp, ignition_path)
        DO UPDATE SET value = EXCLUDED.value;
    """
    minIdx = 0
    maxIdx = batchSize
    
    # DB inserts in batches of N
    while maxIdx <= rows:
        queryData = []
        for row in range(minIdx, maxIdx):
            timestamp = pyDataSet.getValueAt(row, "timestamp")
            value = pyDataSet.getValueAt(row, "value")
            value = round(value, 4) if isinstance(value, (float, int)) else value
            if value is not None:
                # Append (timestamp, customPath, value) for this batch
                queryData.extend([timestamp, customPath, value])
            else:
                print("Skipping row due to None value:", row)
        # Insert N rows into DB
        if queryData is not None:
            system.db.runPrepUpdate(query, queryData, dbConn)
        
        # Advance indices for next N records
        minIdx = maxIdx
        maxIdx += batchSize
    
    # Handle any leftover smaller than batchSize
    if minIdx < rows:
        queryData = []
        for row in range(minIdx, rows):
            timestamp = pyDataSet.getValueAt(row, "timestamp")
            value = pyDataSet.getValueAt(row, "value")
            value = round(value, 4) if isinstance(value, (float, int)) else value
            if value is not None:
                # Append (timestamp, customPath, value) for leftovers
                queryData.extend([timestamp, customPath, value])
            else:
                print("Skipping row due to None value:", row)
        # Insert leftover rows into DB
        if queryData is not None:
            system.db.runPrepUpdate(query, queryData, dbConn)
        

def upsert_db_populate_from_pydataset2(pyDataSet, dbConn, batchSize, customPath):
    rows = pyDataSet.getRowCount()
    query = """
        INSERT INTO records (T_stamp, ignition_path, value)
        VALUES (?, ?, ?)
        ON CONFLICT (T_stamp, ignition_path)
        DO UPDATE SET value = EXCLUDED.value;
    """
    
    for minIdx in range(0, rows, batchSize):
        maxIdx = min(minIdx + batchSize, rows)  # Ensure we don't go out of bounds
        value = pyDataSet.getValueAt(row, "value")
        value = round(value, 4) if isinstance(value, (float, int)) else value
        if value is not None:
            queryData = [
                (pyDataSet.getValueAt(row, "timestamp"), customPath, value)
                for row in range(minIdx, maxIdx)
            ]
        else:
            print("Skipping row due to None value:", row)
        # Flatten queryData for runPrepUpdate (if required)
        flattenedData = [item for sublist in queryData for item in sublist]
        # Insert batch into DB
        if flattenedData is not None:
            system.db.runPrepUpdate(query, flattenedData, dbConn)
        

def log_script_run(path, start_time, finish_time, status, details="", startDate=None, endDate=None):
    query = """
        INSERT INTO script_run_info (path, start_time, finish_time, status, details, start_date, end_date)
        VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    system.db.runPrepUpdate(query, [path, start_time, finish_time, status, details, startDate, endDate], "IQ_Report")
    print("Script run logged with start date:", startDate, "status:", status)
    
def last_run_data(tag_path, db_name):
    query = """
        SELECT end_date 
        FROM script_run_info 
        WHERE status = 'Success' AND path = ? 
        ORDER BY end_date DESC 
        LIMIT 1;
    """
    last_run_data = system.db.runPrepQuery(query, [tag_path], db_name)
    # Check if the dataset is not empty
    if last_run_data.getRowCount() > 0:
        # Extract the first row and 'end_date' column value
        end_date_value = last_run_data.getValueAt(0, "end_date")
        time_diff = system.date.secondsBetween(end_date_value, system.date.now())
        if time_diff < 86400:  # Check if last successful run was within a day
            print("Script won't run since the last successful run was less than a day ago. Time diff is " + str(time_diff))
            return 1
        else:
            return end_date_value
    else:
        return None  # Return None if no data is found



# Step 1: Retrieve the last successful run's end_date for the specific path from the database
last_run_info = last_run_data(path[0], db)
# Step 2: Check if a previous successful run exists
# # Check if last successful run was within a day to avoid the run
if last_run_info == 1:
    pass
else:
    if last_run_info is None:
        print("No matching data found. Using default date.")
        sDate = system.date.midnight(default_date)
    else:
        print("Last end date:", last_run_info)
        sDate = last_run_info

    print("Start date:", sDate)
    # Step 3: Set eDate to sDate + 1 day
    eDate = system.date.addDays(sDate, 1)
    # Step 4: Log the start of this run with the given date range & path
    log_script_run(path[0], sDate, None, "Running", "Script started.", sDate, eDate)

    # Calculate additional variables for query
    numDays = system.date.daysBetween(sDate, eDate)
    timeout = numDays * 1000 * 24 * 12
    returnSize = numDays * 24 * 60
    print("Number of days between dates:", numDays, "Return size:", returnSize, "Timeout:", timeout)
    # Step 5: Retrieve historical data
    data = system.tag.queryTagHistory(
        paths=path, 
        startDate=startDate, 
        endDate=endDate, 
        returnSize=returnSize, 
        aggregationModes=["Average"], 
        returnFormat='Tall', 
        timeout=timeout
    )
    print("Historical data query returned with row count:", data.getRowCount())
    pyDataSet = system.dataset.toPyDataSet(data)
    rowCount = pyDataSet.getRowCount()
    print("Dataset column names:", columnNames, "Total rows:", rowCount)
    ### TODO make loop all + change dynamic avg max or ect, try for insert and verification of inserts 
    # Step 6 insert batch into db3, try catch for inserts or here ? when to control succes 
    upsert_db_populate_from_pydataset(pyDataSet, db, batchSize, path[0])

    # Step 7 Log End time & script Success
    ExecuteEnd = system.date.now()
    log_script_run(path[0], sDate, ExecuteEnd, "Success", "Script started.", sDate, eDate)
    
