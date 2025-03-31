import os
import glob
import shutil
import random
import multiprocessing
import json

# Load configuration from config.json
def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

config = load_config()
NUM_PROCESSES = config["num_processes"]
BASE_INPUT = "./Input_chunk"
BASE_OUTPUT = "./Output"

def process_folder(i):
    input_folder = os.path.join(BASE_INPUT, f"Input_{i}")
    output_folder = os.path.join(BASE_OUTPUT, f"Output_{i}")

    # 1. Gather text files (.txt) in input_folder
    input_txt_paths = glob.glob(os.path.join(input_folder, "*.txt"))
    input_filenames = [os.path.basename(p) for p in input_txt_paths]

    # 2. Gather xmi files in output_folder and strip ".xmi"
    output_xmi_paths = glob.glob(os.path.join(output_folder, "*.xmi"))
    output_filenames = [os.path.basename(p)[:-4] for p in output_xmi_paths]  # remove ".xmi"

    input_set = set(input_filenames)
    output_set = set(output_filenames)

    # 3. Determine which input files are missing corresponding output
    missing_set = input_set - output_set  # Those that do NOT have corresponding XMI
    found_set = input_set & output_set    # Those that DO have corresponding XMI

    found_count = len(found_set)

    # Print how many files are processed and missing for this folder
    missing_count = len(missing_set)
    print(f"[Folder {i}] Processed {found_count} output file(s); Missing {missing_count} output file(s).")

    # Delete the input text files that have corresponding XMI
    for fname in found_set:
        txt_path = os.path.join(input_folder, fname)
        if os.path.exists(txt_path):
            os.remove(txt_path)
    print(f"[Folder {i}] Finished removing {found_count} output file(s)")
    print(f"[Folder {i}] Finished processing.")

def main():
    folder_indices = range(1, NUM_PROCESSES + 1)
    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        pool.map(process_folder, folder_indices)

    print(f"All {NUM_PROCESSES} processes have finished. Pipeline Step 4 complete.")

if __name__ == "__main__":
    main()
