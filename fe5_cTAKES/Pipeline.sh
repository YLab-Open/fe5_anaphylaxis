#!/bin/bash
nohup python3 -u "Pipeline Step 1 - Prepare Input.py" > "Pipeline Step 1 - Prepare Input.log" 2>&1 &
wait
nohup python3 -u "Pipeline Step 2 - Chunk Input.py" > "Pipeline Step 2 - Chunk Input.log" 2>&1 &
wait
nohup bash "./Pipeline Step 3 - Run cTAKES.sh" > "Pipeline Step 3 - Run cTAKES.log" 2>&1 &
wait
nohup python3 -u "Pipeline Step 4 - Remove Processed Note Chunks.py" > "Pipeline Step 4 - Remove Processed Note Chunks.log" 2>&1 &
wait
nohup python3 -u "Pipeline Step 5 - Process Output.py" > "Pipeline Step 5 - Process Output.log" 2>&1 &
wait
echo "Pipeline completed. Please execute 'Post Processing.sh' to generate the final output once all inputs have been processed."