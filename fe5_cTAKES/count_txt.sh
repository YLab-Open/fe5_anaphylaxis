#!/bin/bash

# Define the target directory
TARGET_DIR="./Input_chunk"

# Efficiently count the total number of TXT files (including subdirectories)
CSV_COUNT=$(find "$TARGET_DIR" -type f -name "*.txt" | wc -l)

# Output the result
echo "Total number of TXT files to be processed: $CSV_COUNT"
