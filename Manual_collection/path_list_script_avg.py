list_ETL_AVG_paths = [
    "[hist/iqaluit scada system - wtp:wtp]analog/fit_1001/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_1001/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_1001a/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_1002/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_1001b/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_1001c/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_2101/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_2111/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_2121/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_2131/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_2501/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501a/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501b/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501c/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501d/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501e/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501f/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501g/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_5001/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_5002/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/tit_5001/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501h/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/ait_2501i/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/fit_5001/val",
    "[hist/iqaluit scada system - wtp:wtp]plant_str_lvl_sp",
    "[hist/iqaluit scada system - wtp:wtp]plant_stp_lvl_sp",
    "[hist/iqaluit scada system - wtp:wtp]analog/fit_4001/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_4001/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/fit_4011/val",
    "[hist/iqaluit scada system - wtp:wtp]analog/lit_4011/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/10_fit_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/10_fit_02/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/10_pit_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/10_tit_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/20_fit_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/20_pit_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/20_tit_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/10_ait_02/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/10_ait_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/30_fit_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/30_pit_01/val",
    "[hist/iqaluit scada system - bs1:bs1]analog/30_tit_01/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/fit_311/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/pit_310/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/tit_310/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/ait_313/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/ait_312/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/fit_312/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/fit_310/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/tit_314/val",
    "[hist/iqaluit scada system - bs2:bs2]analog/fit_313/val",
    "[hist/iqaluit scada system - LS1:LS1]analog/lit_101/val",
    "[hist/iqaluit scada system - LS1:LS1]analog/fit_101/val",
    "[hist/iqaluit scada system - LS2:LS2]analog/lit_102/val",
    "[hist/iqaluit scada system - WWTP:WWTP]li_01_100/val",
    "[hist/iqaluit scada system - WWTP:WWTP]FI_01_150/val",
    "[hist/iqaluit scada system - WWTP:WWTP]FI_02_328/val",
    "[hist/iqaluit scada system - WWTP:WWTP]daf1_tss",
    "[hist/iqaluit scada system - WWTP:WWTP]daf2_tss",
    "[hist/iqaluit scada system - WWTP:WWTP]FI_04_300/val",
]
i = 1
for item in list_ETL_AVG_paths:

	dest_path = item
	aggregation_for_this_path = "Average"
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
