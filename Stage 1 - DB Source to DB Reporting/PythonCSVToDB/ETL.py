import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# Database connection configuration
DB_USER = Q
DB_PASSWORD = E
DB_HOST = R
DB_PORT = T
DB_NAME = H
TABLE_NAME = K

# PostgreSQL connection URI
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create a database engine
engine = create_engine(DATABASE_URI)

# Check database connection
try:
    with engine.connect() as connection:
        print("Successfully connected to the database.")
except OperationalError as e:
    print("Failed to connect to the database. Please check your configuration.")
    print(e)
    exit()

# Path to the parent folder containing the CSV files
folder_path = r"C:\Users\Marsh\Desktop\Work\PowerBi Data till start - 2024-12-02 end\Insertdata"

# Iterate through all files in the folder and subfolders
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith(".csv"):
            file_path = os.path.join(root, file)
            print(f"Processing file: {file_path}")

            # Read CSV, keeping only the necessary columns
            try:
                df = pd.read_csv(file_path, usecols=["path", "value", "timestamp"])
                # Rename columns to match the database schema
                df.rename(columns={"path": "ignition_path", "timestamp": "t_stamp"}, inplace=True)
                # Append data to the database
                df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
                print(f"Successfully added data from {file} to the database.")
            except Exception as e:
                print(f"Failed to process {file_path}: {e}")

print("Data migration completed.")
