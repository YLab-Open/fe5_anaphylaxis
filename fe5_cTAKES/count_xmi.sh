#!/bin/bash

# Define the target directory
TARGET_DIR="./Output"

# Efficiently count the total number of XMI files (including subdirectories)
CSV_COUNT=$(find "$TARGET_DIR" -type f -name "*.xmi" | wc -l)

# Output the result
echo "Total number of processed XMI files: $CSV_COUNT"
