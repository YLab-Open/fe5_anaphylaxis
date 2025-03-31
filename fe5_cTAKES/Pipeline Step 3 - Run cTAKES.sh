#!/bin/bash

# Load config values from config.json
CONFIG_FILE="config.json"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Configuration file $CONFIG_FILE not found!"
  exit 1
fi

USER=$(jq -r '.UMLS_username' "$CONFIG_FILE")
PASS=$(jq -r '.UMLS_password' "$CONFIG_FILE")
KEY=$(jq -r '.UMLS_API_key' "$CONFIG_FILE")
PROCESS=$(jq -r '.num_processes' "$CONFIG_FILE")
INPUT="./Input_chunk"
OUTPUT="./Output"

# Record the start time
start_time=$(date +%s)

# Get all input folders
input_folders=($(ls -d ${INPUT}/Input_* 2>&1))
num_folders=${#input_folders[@]}

if [ $num_folders -eq 0 ]; then
  echo "Error: No input folders found. Please ensure that you have run the 'Step 1 - Prepare Input.py'."
  exit 1
fi

echo "Found $num_folders input folders. Processing with $PROCESS parallel jobs."

# Ensure the cTAKES directory exists
if [ ! -d "./cTAKES" ]; then 
    mkdir -p "./cTAKES" 
fi

for id in $(seq 1 $((num_folders)))
do
  # Check if ./cTAKES/apache-ctakes-4.0.0.1_${id} does not exist
  if [ ! -d "./cTAKES/apache-ctakes-4.0.0.1_${id}" ]; then  
      cp -r apache-ctakes-4.0.0.1 ./cTAKES/apache-ctakes-4.0.0.1_${id}
  fi
done

# Initialize counters
running_jobs=0
folder_index=0

# Function to clean up finished processes
check_jobs() {
  while [ $running_jobs -ge $PROCESS ]; do
    wait -n  # Wait for any background process to finish
    running_jobs=$((running_jobs - 1))
  done
}

# Process each input folder
while [ $folder_index -lt $num_folders ]; do
  check_jobs  # Ensure we don't exceed the max number of parallel processes

  input_folder=${input_folders[$folder_index]}
  id=$(echo "$input_folder" | grep -oE '[0-9]+$')  # Extract folder ID  

  # Ensure the output directory exists
  if [ ! -d "${OUTPUT}/Output_${id}" ]; then 
      mkdir -p "${OUTPUT}/Output_${id}" 
  fi

  # Run the command for each folder in the background
  ./cTAKES/apache-ctakes-4.0.0.1_${id}/bin/runClinicalPipeline.sh \
    -i ${input_folder} \
    --xmiOut "../../${OUTPUT}/Output_${id}" \
    --user $USER \
    --pass $PASS \
    --key $KEY &
  
  echo "Started processing ${input_folder}"

  # Increment counters
  running_jobs=$((running_jobs + 1))
  folder_index=$((folder_index + 1))

done

# Wait for remaining processes to finish
wait

# Record the end time
end_time=$(date +%s)

# Calculate the total execution time in seconds
execution_time=$((end_time - start_time))

echo "All processes have completed cTAKES annotation. Pipeline Step 3 complete."
echo "Total execution time: ${execution_time} seconds."


