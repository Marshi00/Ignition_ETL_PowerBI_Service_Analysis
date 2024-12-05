import os
import pandas as pd

# Define the base folder path
base_folder = "C:/Users/Marsh/Desktop/Work/PowerBi Data till start - 2024-12-02 end/AVG"

# Check if the base folder exists
if not os.path.exists(base_folder):
    print(f"The base folder '{base_folder}' does not exist.")
    exit(1)

# Iterate through each subfolder
for i in range(1, 61):  # Subfolders numbered from 1 to 27
    subfolder_path = os.path.join(base_folder, str(i))
    if not os.path.exists(subfolder_path):
        print(f"Subfolder {subfolder_path} does not exist. Skipping.")
        continue

    # List all CSV files in the subfolder
    csv_files = [file for file in os.listdir(subfolder_path) if file.endswith('.csv')]
    if len(csv_files) < 2:
        print(f"Subfolder {subfolder_path} does not contain enough CSV files to merge. Skipping.")
        continue

    # Merge the CSV files
    merged_data = pd.concat([pd.read_csv(os.path.join(subfolder_path, file)) for file in csv_files])

    # Name the merged file as '6_all.csv'
    merged_file_name = "6_all.csv"
    merged_file_path = os.path.join(subfolder_path, merged_file_name)

    # Save the merged file
    merged_data.to_csv(merged_file_path, index=False)
    print(f"Merged file saved as {merged_file_path}")
