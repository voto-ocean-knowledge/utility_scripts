#!/bin/bash
# script to process a single mission in the background and log the progress
nohup /data/miniconda/envs/process/bin/python /home/pipeline/utility_scripts/pyglider_nrt_from_complete.py $1 $2 raw > /data/log/error_nrt_from_complete.log 2>&1 &