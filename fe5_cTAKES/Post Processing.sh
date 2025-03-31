#!/bin/bash
nohup python3 -u "Post Processing Step 1 - Aggregate Output.py" > "Post Processing Step 1 - Aggregate Output.log" 2>&1 &
wait
nohup python3 -u "Post Processing Step 2 - Generate Note Level Results.py" > "Post Processing Step 2 - Generate Note Level Results.log" 2>&1 &
wait
nohup python3 -u "Post Processing Step 3 - Generate Final Results.py" > "Post Processing Step 3 - Generate Final Results.log" 2>&1 &
wait
echo "Post Processing completed. Please see fe_feature_table_obesity.csv and fe_feature_table_substance_abuse.csv for final results."