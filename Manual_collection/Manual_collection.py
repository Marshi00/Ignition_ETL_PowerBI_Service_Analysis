dest_path = N
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
    filePath = V + startDateSafe + "_" + endDateSafe + "_fqi_312.csv"

    # Write the file
    system.file.writeFile(filePath, csv)

    # Move the start date forward by one year for the next loop iteration
    sDate = eDate

# End time of script execution
ExecuteEnd = system.date.now()
print "Execute Script End Time: ", ExecuteEnd
print "Total Execution Time: ", system.date.secondsBetween(ExecuteStart, ExecuteEnd), " seconds"
