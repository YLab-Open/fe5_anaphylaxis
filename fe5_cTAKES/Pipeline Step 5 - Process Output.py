import os
import glob
import tarfile
import pandas as pd
import multiprocessing
import xml.etree.ElementTree as ET
import json

# Load configuration from config.json
def load_config(config_path="config.json"):
    with open(config_path, "r") as f:
        return json.load(f)

config = load_config()

NUM_PROCESSES = config["num_processes"]
BASE_OUTPUT = "./Output"
BASE_RESULT = "./Result"

def get_cui_list(feature):
    cui_list = []
    with open(f'./CUI/{feature}_umls_cui_clean.txt', 'r') as file:
        lines = file.readlines()
        for line in lines:
            cui_list.append(line.split("|")[-1].strip())
    return cui_list

def assign_status(xmi_file, cui_list):
    status = "U"
    order = {"A": 5, "N": 4, "H": 3, "X": 2, "U": 1}
    # Load and parse the XMI file
    tree = ET.parse(xmi_file)
    root = tree.getroot()
    
    # Define namespace map
    namespaces = {
        'refsem': 'http:///org/apache/ctakes/typesystem/type/refsem.ecore',
        'textsem': 'http:///org/apache/ctakes/typesystem/type/textsem.ecore'
    }
    
    # Extract xmi:id
    target_ids = []
    for concept in root.findall(".//refsem:UmlsConcept", namespaces):
        cui = concept.get('cui')
        if cui in cui_list:
            target_ids.append(concept.get('{http://www.omg.org/XMI}id'))
    
    # Get polarity, conditional, subject, and historyOf from textsem:DiseaseDisorderMention fields
    for target_id in target_ids:
        for mention in root.findall(".//textsem:DiseaseDisorderMention", namespaces):
            ontology_concepts = mention.get('ontologyConceptArr')
            if ontology_concepts and target_id in ontology_concepts.split():
                confidence = mention.get('confidence')
                polarity = mention.get('polarity')
                conditional = mention.get('conditional')
                subject = mention.get('subject')
                historyOf = mention.get('historyOf')
                if polarity == '1' and conditional == 'false' and subject == 'patient' and historyOf == '0':
                    status = "A"
                    return status
                elif polarity == '-1' and conditional == 'false' and subject == 'patient' and historyOf == '0':
                    status = "N" if order[status] < order["N"] else status
                elif polarity == '1' and conditional == 'false' and subject == 'patient' and historyOf == '1':
                    status = "H" if order[status] < order["H"] else status
                elif polarity == '1' and conditional == 'false' and subject != 'patient' and historyOf == '0':
                    status = "X" if order[status] < order["X"] else status
                else:
                    status = "U" if order[status] < order["U"] else status

    return status

def ensure_directory_exists(path):
    """Ensure that `path` directory exists; if not, create it."""
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def parse_filename(xmi_file):
    """
    Given an XMI file path with a name like:
        {patient_id}_{encounter_id}_{note_id}_{note_date}_{provider_id}(.{chunk_id})?.txt.xmi
    return (patient_id, encounter_id, note_id, note_date, provider_id).
    The chunk_id is optional and is ignored if present.
    """
    basename = os.path.basename(xmi_file)
    # Remove trailing ".txt.xmi" (8 characters)
    if basename.endswith(".txt.xmi"):
        basename = basename[:-8]

    parts = basename.split("_")
    # 5 parts => patient_id, encounter_id, note_id, note_date, provider_id
    # 6 parts => + chunk_id
    if len(parts) == 5:
        patient_id, encounter_id, note_id, note_date, provider_id = parts
    elif len(parts) == 6:
        patient_id, encounter_id, note_id, note_date, provider_id, _ = parts
    else:
        raise ValueError(f"Unexpected filename format: {os.path.basename(xmi_file)}")

    return patient_id, encounter_id, note_id, note_date, provider_id

def process_output_folder(folder_index):
    """
    Incrementally process new XMI files under ./Output/Output_{folder_index}.
    1) Create (if needed) ./Result/Result_{folder_index}.
    2) For each new XMI file, parse the name, create 1 anaphylaxis row.
    3) Append the XMI files to the existing or new tar archive (./Output/Output_{folder_index}.tar), then remove the XMI file.
    4) Append the new rows to existing or new CSVs:
         ./Result/fe_feature_detail_table_anaphylaxis_{folder_index}.csv
    5) Remove the tar archive to save space if no errors occurred.
    """
    output_folder = os.path.join(BASE_OUTPUT, f"Output_{folder_index}")
    result_folder = os.path.join(BASE_RESULT, f"Result_{folder_index}")
    ensure_directory_exists(result_folder)
    result_folder_anaphylaxis = os.path.join(result_folder, "anaphylaxis")
    ensure_directory_exists(result_folder_anaphylaxis)

    xmi_files = glob.glob(os.path.join(output_folder, "*.xmi"))
    if not xmi_files:
        print(f"[Output_{folder_index}] No new XMI files found.")
        return

    # We'll store the new rows for anaphylaxis in lists
    anaphylaxis_rows = []

    # Append or create the existing tar file
    tar_name = f"Output_{folder_index}.tar"
    tar_path = os.path.join(output_folder, tar_name)

    # open in append mode 'a' => uncompressed tar only
    with tarfile.open(tar_path, mode="a") if os.path.exists(tar_path) else tarfile.open(tar_path, mode="w") as tar:
        for xmi_path in xmi_files:
            patient_id, encounter_id, note_id, note_date, provider_id = parse_filename(xmi_path)

            # ----- Anaphylaxis row -----
            anaphylaxis_cui_list = get_cui_list("anaphylaxis")
            try:
                anaphylaxis_status = assign_status(xmi_path, anaphylaxis_cui_list)
            except:
                anaphylaxis_status = "U"
            anaphylaxis_row = [
                patient_id,         # PatID
                encounter_id,       # EncounterID
                note_id,            # NoteID
                1004,               # FeatureID for anaphylaxis
                note_date,          # Feature_dt
                "C0002792",         # Feature
                "UC",               # FE_CodeType
                provider_id,        # ProviderID
                "N",                # Confidence
                anaphylaxis_status      # Feature_Status
            ]
            anaphylaxis_rows.append(anaphylaxis_row)

            # Add file to the tar archive
            tar.add(xmi_path, arcname=os.path.basename(xmi_path))
            # Delete the original file
            # os.remove(xmi_path)

    # Build DataFrames for newly processed rows
    columns = [
        "PatID", "EncounterID", "NoteID", "FeatureID", "Feature_dt", "Feature",
        "FE_CodeType", "ProviderID", "Confidence", "Feature_Status"
    ]
    df_anaphylaxis_new = pd.DataFrame(anaphylaxis_rows, columns=columns)

    # CSV paths
    anaphylaxis_csv_path = os.path.join(result_folder_anaphylaxis, f"fe_feature_detail_table_anaphylaxis_{folder_index}.csv")

    # ----- Append or create anaphylaxis CSV -----
    # If file doesn't exist, write with header. If it does, append without header.
    if os.path.exists(anaphylaxis_csv_path):
        df_anaphylaxis_new.to_csv(anaphylaxis_csv_path, mode="a", header=False, index=False)
    else:
        df_anaphylaxis_new.to_csv(anaphylaxis_csv_path, mode="w", header=True, index=False)

    print(f"[Output_{folder_index}] Processed {len(xmi_files)} new XMI file(s). Appended to '{tar_name}' and CSVs.")

    os.remove(tar_path) # Remove the tar archive to save space. You may choose to comment this line out so that the output files are recoverable, but be warned, the tar files can be quite large.

def main():
    folder_indices = range(1, NUM_PROCESSES + 1)
    with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
        pool.map(process_output_folder, folder_indices)

    print(f"All {NUM_PROCESSES} processes have finished processing output. Pipeline Step 5 complete.")

if __name__ == "__main__":
    main()
