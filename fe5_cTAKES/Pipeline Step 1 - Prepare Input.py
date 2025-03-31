import os
import pandas as pd
import regex
import json
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Load configuration from config.json
def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

def remove_invalid_xml_chars(text):
    """
    Replaces invalid XML 1.0 characters in a given string with spaces.
    """
    valid_xml_10_re = regex.compile(
        r'[\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]'
    )

    return ''.join(c if valid_xml_10_re.match(c) else ' ' for c in text)

def process_csv(args):
    """Processes a single CSV file and saves rows as text files in designated folders."""
    input_file, num_folders, patient_id_col, encounter_id_col, note_id_col, note_date_col, provider_id_col, note_text_col = args

    try:
        df = pd.read_csv(input_file)

        df[patient_id_col] = df[patient_id_col].astype(str)
        df[encounter_id_col] = df[encounter_id_col].astype(str)
        df[note_id_col] = df[note_id_col].astype(str)
        df[note_date_col] = df[note_date_col].astype(str)
        df[provider_id_col] = df[provider_id_col].astype(str)
        df[note_text_col] = df[note_text_col].astype(str)

        for row_id, row in df.iterrows():
            try:
                patient_id = row[patient_id_col]
                encounter_id = row[encounter_id_col]
                note_id = row[note_id_col]
                note_date = row[note_date_col]
                provider_id = row[provider_id_col]
                note_text = row[note_text_col]

                note_text = remove_invalid_xml_chars(note_text)

                folder_index = row_id % num_folders + 1
                output_folder = os.path.join("Input", f"Input_{folder_index}")

                os.makedirs(output_folder, exist_ok=True)

                output_file_name = f"{patient_id}_{encounter_id}_{note_id}_{note_date}_{provider_id}.txt"
                output_file_path = os.path.join(output_folder, output_file_name)
                with open(output_file_path, "w", encoding="utf-8") as file:
                    file.write(str(note_text))
            except Exception as e:
                print(f"Error processing row {row_id} in file {input_file}: {e}")

        return f"Processed file: {input_file}"  # Return message for tqdm tracking

    except Exception as e:
        return f"Error processing file {input_file}: {e}"  # Return error message for tqdm

def main(config_path="config.json"):
    config = load_config(config_path)

    clinical_notes_dir = config["clinical_notes_directory"]
    patient_id_col = config["patient_id_column_name"]
    encounter_id_col = config["encounter_id_column_name"]
    note_id_col = config["note_id_column_name"]
    note_date_col = config["note_date_column_name"]
    provider_id_col = config["provider_id_column_name"]
    note_text_col = config["note_text_column_name"]
    num_processes = config["num_processes"]
    num_folders = num_processes

    input_main_folder = "Input"
    os.makedirs(input_main_folder, exist_ok=True)

    output_main_folder = "Output"
    os.makedirs(output_main_folder, exist_ok=True)

    for i in range(1, num_folders + 1):
        os.makedirs(os.path.join(input_main_folder, f"Input_{i}"), exist_ok=True)

    for i in range(1, num_folders + 1):
        os.makedirs(os.path.join(output_main_folder, f"Output_{i}"), exist_ok=True)

    csv_files = sorted([os.path.join(clinical_notes_dir, f) for f in os.listdir(clinical_notes_dir) if f.endswith(".csv")])
    print(f"Found {len(csv_files)} CSV files.")

    args = [
        (file, num_folders, patient_id_col, encounter_id_col, note_id_col, note_date_col, provider_id_col, note_text_col)
        for file in csv_files
    ]

    with Pool(processes=num_processes) as pool:
        # Use tqdm with `imap_unordered` for better progress tracking
        for _ in tqdm(pool.imap_unordered(process_csv, args), total=len(args), desc="Processing CSV files", unit="file"):
            pass  # tqdm automatically updates progress

    print(f"All {num_processes} processes have finished processing the input. Pipeline Step 1 complete.")

if __name__ == "__main__":
    main()
