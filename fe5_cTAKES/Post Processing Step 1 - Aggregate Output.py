import os
import glob
import pandas as pd
import multiprocessing
import json

# Load configuration from config.json
def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

config = load_config()

NUM_PROCESSES = config["num_processes"]

# The custom ranking for Feature_Status
STATUS_ORDER = {"A": 5, "N": 4, "H": 3, "X": 2, "U": 1}

def aggregate_csv_file(csv_file):
    """
    Read the CSV file as a pandas DataFrame, group by all columns except 'Feature_Status'
    and, for each group, choose the Feature_Status with the highest rank.
    Save the aggregated DataFrame to a new file named "{original_csv_filename}_aggregated.csv"
    in the same directory.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
        return

    if 'Feature_Status' not in df.columns:
        print(f"Skipping {csv_file}: 'Feature_Status' column not found.")
        return

    # Identify the columns to group by (all columns except 'Feature_Status')
    grouping_cols = [col for col in df.columns if col != "Feature_Status"]

    try:
        # Group the DataFrame and, for each group, select the maximum Feature_Status according to STATUS_ORDER.
        # The lambda function uses STATUS_ORDER to determine the rank; if a value is not found, 0 is used.
        aggregated_df = df.groupby(grouping_cols, as_index=False).agg({
            "Feature_Status": lambda s: max(s, key=lambda x: STATUS_ORDER.get(x, 0))
        })
    except Exception as e:
        print(f"Error aggregating {csv_file}: {e}")
        return

    csv_file = csv_file[:-4] # Remove ".csv" extension
    # Build the output filename by appending "_aggregated.csv" to the original filename.
    output_csv = f"{csv_file}_aggregated.csv"
    try:
        aggregated_df.to_csv(output_csv, index=False)
        print(f"Aggregated CSV saved to {output_csv}")
    except Exception as e:
        print(f"Error saving aggregated CSV for {csv_file}: {e}")

def process_result_folder(folder_index):
    """
    For result folder Result_X:
      - Find all CSV files (excluding ones already aggregated).
      - For each CSV file, perform the aggregation.
    """
    result_folder_anaphylaxis = f"./Result/Result_{folder_index}/anaphylaxis"
    # Find all CSV files in the folder (skip files that have already been aggregated)
    csv_files = glob.glob(os.path.join(result_folder_anaphylaxis, "*.csv"))
    csv_files = [f for f in csv_files if not f.endswith("_aggregated.csv")]

    if not csv_files:
        print(f"[Result_{folder_index}] No CSV files to process.")
        return

    for csv_file in csv_files:
        aggregate_csv_file(csv_file)
    print(f"[Result_{folder_index}] Processed {len(csv_files)} CSV file(s).")

def main():
    folder_indices = range(1, NUM_PROCESSES + 1)
    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        pool.map(process_result_folder, folder_indices)
    print("Aggregation completed for all result folders. Post Processing Step 1 complete.")

if __name__ == "__main__":
    main()
