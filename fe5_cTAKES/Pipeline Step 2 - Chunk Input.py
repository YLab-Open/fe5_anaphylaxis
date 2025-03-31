import os
import glob
import multiprocessing
import json

# Load configuration from config.json
def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

config = load_config()
CHUNK_SIZE_BYTES = config["note_chunk_size_bytes"]
NUM_PROCESSES = config["num_processes"]
BASE_INPUT = "./Input"
BASE_OUTPUT = "./Input_chunk"

def ensure_directory_exists(path: str):
    """Ensure that `path` directory exists; if not, create it."""
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def chunk_file(input_path: str, output_folder: str, chunk_size_bytes: int = CHUNK_SIZE_BYTES):
    """
    Read a text file line by line, group lines into chunks up to `chunk_size_bytes`,
    and write each chunk to a new file under `output_folder`.
    Each chunk is named {original_filename}_{chunk_id}.txt.
    
    A single line larger than `chunk_size_bytes` will occupy a chunk by itself.
    """
    ensure_directory_exists(output_folder)

    # e.g., if input_path = ".../somefile.txt", original_name = "somefile.txt"
    original_name = os.path.basename(input_path)
    base_name, ext = os.path.splitext(original_name)  # e.g. ("somefile", ".txt")

    chunk_id = 1
    buffer_lines = []
    buffer_size = 0  # track bytes in current chunk

    with open(input_path, "r", encoding="utf-8") as infile:
        for line in infile:
            # Calculate the size (in bytes) of this line (including the newline if present)
            line_bytes = len(line.encode("utf-8"))

            # If adding this line would exceed chunk_size_bytes, flush what we have so far
            # BUT if buffer is empty, we have to put this line alone in a chunk (even if > chunk_size_bytes).
            if buffer_lines and (buffer_size + line_bytes > chunk_size_bytes):
                # Write current buffer to a chunk file
                out_name = f"{original_name}_{chunk_id}.txt"
                out_path = os.path.join(output_folder, out_name)
                with open(out_path, "w", encoding="utf-8") as out_f:
                    out_f.writelines(buffer_lines)

                chunk_id += 1
                buffer_lines = []
                buffer_size = 0

            # Now add the current line to the buffer (even if it alone exceeds chunk_size_bytes).
            buffer_lines.append(line)
            buffer_size += line_bytes

        # After reading all lines, if anything remains in the buffer, flush it
        if buffer_lines:
            out_name = f"{original_name}_{chunk_id}.txt"
            out_path = os.path.join(output_folder, out_name)
            with open(out_path, "w", encoding="utf-8") as out_f:
                out_f.writelines(buffer_lines)

def process_input_folder(folder_index: int):
    """
    Process all .txt files in ./Input/Input_{folder_index}.
    Chunk each file into ~5 KB pieces (line boundary–aware) and write them to
    ./Input_chunk/Input_{folder_index}.
    """
    input_folder = os.path.join(BASE_INPUT, f"Input_{folder_index}")
    output_folder = os.path.join(BASE_OUTPUT, f"Input_{folder_index}")

    ensure_directory_exists(output_folder)

    txt_files_before = glob.glob(os.path.join(input_folder, "*.txt"))
    for txt_file in txt_files_before:
        chunk_file(txt_file, output_folder, CHUNK_SIZE_BYTES)

    txt_files_after = glob.glob(os.path.join(output_folder, "*.txt"))
    print(f"[Folder {folder_index}] Finished chuncking {len(txt_files_before)} file(s) into {len(txt_files_after)} file(s).")

    # Delte the original input files
    for txt_file in txt_files_before:
        os.remove(txt_file)
    print(f"[Folder {folder_index}] Finished removing {len(txt_files_before)} input files")
    print(f"[Folder {folder_index}] Finished processing.")

def main():
    folder_indices = range(1, NUM_PROCESSES + 1)

    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        pool.map(process_input_folder, folder_indices)

    print(f"All {NUM_PROCESSES} processes have finished chunking the input. Pipeline Step 2 complete.")

if __name__ == "__main__":
    main()
