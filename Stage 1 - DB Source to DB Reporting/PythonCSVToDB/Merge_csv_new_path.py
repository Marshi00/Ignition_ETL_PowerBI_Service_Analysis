import os
import pandas as pd

# Define the path to the main folder
main_folder_path = r"C:/Users/Marsh/Desktop/Work/PowerBi Data till start - 2024-12-02 end/AVG"

# List of replacement values for the "path" column
replacement_values = [
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

# Iterate through each subfolder (1 to 27)
for folder_number in range(1, 61):
    subfolder_path = os.path.join(main_folder_path, str(folder_number))
    csv_file_path = os.path.join(subfolder_path, "6_all.csv")

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
