import pandas as pd
import psycopg2

# Database connection parameters
db_params = {
    "host": "Q2",  # Change to your database host
    "database": "E2",  # Replace with your database name
    "user": "R2",  # Replace with your PostgreSQL username
    "password": "Y2",  # Replace with your PostgreSQL password
    "port": "T2"  # Default PostgreSQL port
}


# Path to the Excel file
excel_file_path = r"C:/Users/IQ-Re/OneDrive/Desktop/Tag_Details.xlsx"

conn = None  # Initialize the connection variable

try:
    # Read Excel file into a pandas DataFrame
    df = pd.read_excel(excel_file_path, engine='openpyxl')

    # Rename DataFrame columns to match the table column names
    df.columns = [
        "tagname", 
        "description", 
        "short_description", 
        "unit", 
        "path", 
        "aggregation", 
        "site", 
        "type", 
        "manufacture", 
        "model_number", 
        "calculated_flow", 
        "water_use", 
        "water_model", 
        "thermal_model", 
        "operations_all", 
        "operations_water", 
        "operations_wastewater", 
        "regulation_water", 
        "regulation_wastewater", 
        "maintenance", 
        "health_and_safety", 
        "building_condition", 
        "placeholder_1", 
        "placeholder_2", 
        "placeholder_3", 
        "placeholder_4"
    ]

    # Connect to PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Iterate over the DataFrame and insert data into the database
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO public.tag_details (
                tagname, 
                description, 
                short_description, 
                unit, 
                path_details, 
                aggregation, 
                site, 
                type_details, 
                manufacture, 
                model_number, 
                calculated_flow, 
                water_use, 
                water_model, 
                thermal_model, 
                operations_all, 
                operations_water, 
                operations_wastewater, 
                regulation_water, 
                regulation_wastewater, 
                maintenance, 
                health_and_safety, 
                building_condition, 
                placeholder_1, 
                placeholder_2, 
                placeholder_3, 
                placeholder_4
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    # Commit the transaction
    conn.commit()

    print("Data inserted successfully!")

except Exception as e:
    print("Error:", e)

finally:
    # Close the database connection safely
    if conn:
        cursor.close()
        conn.close()
