import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Database connection configuration

DB_USER = Q
DB_PASSWORD = E
DB_HOST = R
DB_PORT = T
DB_NAME = H
TABLE_NAME = K


# PostgreSQL connection URI
DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

# Path to the folder containing the CSV files
folder_path = r"C:\Users\Marsh\Desktop\Work\PowerBi Data till start - 2024-12-02 end\Insertdata"

# File to store failed rows
failed_rows_file = os.path.join(folder_path, "failed_rows.csv")

# Ensure the failed rows file exists and is ready for appending
if os.path.exists(failed_rows_file):
    os.remove(failed_rows_file)

BATCH_SIZE = 1000  # Number of rows to insert in a batch

insert_query = f"""
INSERT INTO {TABLE_NAME} (ignition_path, value, t_stamp)
VALUES (:ignition_path, :value, :t_stamp)
ON CONFLICT (ignition_path, t_stamp) 
DO UPDATE SET value = EXCLUDED.value;
"""

for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith(".csv"):
            file_path = os.path.join(root, file)
            print(f"Processing file: {file_path}")

            try:
                # Read the CSV file
                df = pd.read_csv(file_path, usecols=["path", "value", "timestamp"])
                # Rename columns to match the database schema
                df.rename(columns={"path": "ignition_path", "timestamp": "t_stamp"}, inplace=True)

                # Batch processing
                for start in range(0, len(df), BATCH_SIZE):
                    batch = df.iloc[start:start + BATCH_SIZE]
                    try:
                        # Convert batch to a list of dictionaries
                        data = batch.to_dict(orient="records")
                        with engine.begin() as conn:  # Efficient transaction handling
                            conn.execute(text(insert_query), data)
                    except SQLAlchemyError as e:
                        print(f"Batch failed: {e}")
                        # Append failed rows directly to the CSV file
                        batch.to_csv(failed_rows_file, mode='a', index=False, header=not os.path.exists(failed_rows_file))

            except pd.errors.ParserError as e:
                print(f"Error parsing CSV file {file_path}: {e}. Please inspect this file.")
            except Exception as e:
                print(f"Unexpected error processing {file_path}: {e}")

print(f"Data migration completed. Check {failed_rows_file} for failed rows (if any).")
