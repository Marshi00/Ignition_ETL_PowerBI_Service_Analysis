list_ETL_Max_paths = [
    "[hist/iqaluit scada system - wtp:wtp]totalizer/fqi_1001",
    "[hist/iqaluit scada system - wtp:wtp]valve/fv_2501/val_fdbk",
    "[hist/iqaluit scada system - wtp:wtp]totalizer/fqi_5001",
    "[hist/iqaluit scada system - bs1:bs1]motor/20_p_1/val_speedfdbk",
    "[hist/iqaluit scada system - bs1:bs1]motor/20_p_2/val_speedfdbk",
    "[hist/iqaluit scada system - bs1:bs1]motor/20_p_3/val_speedfdbk",
    "[hist/iqaluit scada system - bs1:bs1]motor/20_p_105/high speed",
    "[hist/iqaluit scada system - bs1:bs1]motor/20_p_1/sts_running",
    "[hist/iqaluit scada system - bs1:bs1]motor/20_p_2/sts_running",
    "[hist/iqaluit scada system - bs1:bs1]motor/20_p_3/sts_running",
    "[hist/iqaluit scada system - bs1:bs1]motor/20_p_105/run status",
    "[hist/iqaluit scada system - bs2:bs2]totalizer/_opc/fqi_311/fqi_311",
    "[hist/iqaluit scada system - bs2:bs2]totalizer/_opc/fqi_312/fqi_312",
    "[hist/iqaluit scada system - bs2:bs2]totalizer/_opc/fqi_310/fqi_310",
    "[hist/iqaluit scada system - bs2:bs2]totalizer/_opc/fqi_312/fqi_313",
    "[hist/iqaluit scada system - bs2:bs2]motor/p301/val_speedfdbk",
    "[hist/iqaluit scada system - bs2:bs2]motor/p302/val_speedfdbk",
    "[hist/iqaluit scada system - bs2:bs2]motor/p303/val_speedfdbk",
    "[hist/iqaluit scada system - bs2:bs2]motor/p304/val_speedfdbk",
    "[hist/iqaluit scada system - bs2:bs2]motor/p301/sts_running",
    "[hist/iqaluit scada system - bs2:bs2]motor/p302/sts_running",
    "[hist/iqaluit scada system - bs2:bs2]motor/p303/sts_running",
    "[hist/iqaluit scada system - bs2:bs2]motor/p304/sts_running",
    "[hist/iqaluit scada system - bs2:bs2]diesel pump/p305/sts_running",
    "[hist/iqaluit scada system - bs2:bs2]discrete/pcv_306_closed/sts",
    "[hist/iqaluit scada system - WWTP:WWTP]yi_02_310/sts",
    "[hist/iqaluit scada system - WWTP:WWTP]yi_02_320/sts",
]
i = 1
for item in list_ETL_Max_paths:

	dest_path = item
	aggregation_for_this_path = "Maximum"
	# Start time of script execution
	ExecuteStart = system.date.now()
	print "Execute Script Start Time: ", ExecuteStart
	
	# Define initial start and end dates for looping by year
	sDate = system.date.getDate(2020, 1, 1)
	finalEndDate = system.date.getDate(2024, 12, 02)
	
	while sDate < finalEndDate:
	    # Set end date to one year after the start date or final end date, whichever is earlier
	    eDate = system.date.addYears(sDate, 1)
	    if eDate > finalEndDate:
	        eDate = finalEndDate
	
	    # Format dates to a readable format
	    startDate = system.date.format(sDate, "yyyy-MM-dd HH:mm:ss")
	    endDate = system.date.format(eDate, "yyyy-MM-dd HH:mm:ss")
	
	    # Calculate the number of days between the dates
	    numDays = system.date.daysBetween(sDate, eDate)
	    timeout = numDays * 1000 * 24 * 60  # Set timeout based on days
	
	    # Set return size for 1-minute intervals
	    returnSize = numDays * 24 * 60
	
	    # Print debug info
	    print "Start Date: ", startDate
	    print "End Date: ", endDate
	    print "Number of Days: ", numDays
	    print "Script Timeout: ", timeout
	    print "Return Size: ", returnSize
	
	    # Define paths to query
	    paths = [
	        dest_path,
	    ]
	
	    # Define aggregation modes
	    aggregationModes = [
	        aggregation_for_this_path,  # Change to other modes like "MinMax" based on need
	    ]
	
	    # Query historical data
	    data = system.tag.queryTagHistory(
	        paths=paths, 
	        startDate=startDate, 
	        endDate=endDate, 
	        returnSize=returnSize, 
	        aggregationModes=aggregationModes, 
	        returnFormat='Tall', 
	        timeout=timeout
	    )
	
	    # Extract column names as a proper list of strings
	    columnNames = list(data.getColumnNames())
	    print "Column Names: ", columnNames
	
	    # Convert the dataset to a Python dataset for manipulation
	    pyDataSet = system.dataset.toPyDataSet(data)
	
	    # Round numeric values to 2 decimal places
	    roundedRows = [
	        [round(value, 4) if isinstance(value, (float, int)) else value for value in row]
	        for row in pyDataSet
	    ]
	
	    # Create a new dataset with formatted data
	    formattedData = system.dataset.toDataSet(columnNames, roundedRows)
	
	    # Convert the dataset to CSV format
	    csv = system.dataset.toCSV(formattedData)
	
	    # Replace ':' with '_' in formatted dates for a valid file name
	    startDateSafe = startDate.replace(":", "_")
	    endDateSafe = endDate.replace(":", "_")
	
	    # Create the dynamic file path with formatted dates
	    filePath = r"C:/Users/IQ-Re/OneDrive/Desktop/Data/" + str(i)+"_" +startDateSafe + "_" + endDateSafe + ".csv"
	
	    # Write the file
	    system.file.writeFile(filePath, csv)
	
	    # Move the start date forward by one year for the next loop iteration
	    sDate = eDate
	
	# End time of script execution
	ExecuteEnd = system.date.now()
	print "Execute Script End Time: ", ExecuteEnd
	print "Total Execution Time: ", system.date.secondsBetween(ExecuteStart, ExecuteEnd), " seconds"
	i = i + 1
