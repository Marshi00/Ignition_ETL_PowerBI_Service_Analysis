import os
import pandas as pd

# Define the path to the main folder
main_folder_path = D

# List of replacement values for the "path" column
replacement_values = S

# Iterate through each subfolder (1 to 27)
for folder_number in range(1, 61):
    subfolder_path = os.path.join(main_folder_path, str(folder_number))
    csv_file_path = os.path.join(subfolder_path, G)

    # Check if the CSV file exists
    if os.path.exists(csv_file_path):
        # Read the CSV file
        df = pd.read_csv(csv_file_path)

        # Replace all values in the "path" column
        if "path" in df.columns:
            df["path"] = replacement_values[folder_number - 1]

            # Save the updated CSV file with the new name
            new_csv_file_path = os.path.join(subfolder_path, f"{folder_number}7_new_path.csv")
            df.to_csv(new_csv_file_path, index=False)
            print(f"Processed and saved: {new_csv_file_path}")
        else:
            print(f"'path' column not found in: {csv_file_path}")
    else:
        print(f"File not found: {csv_file_path}")
