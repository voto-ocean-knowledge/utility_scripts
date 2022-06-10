#!/bin/bash
# script to process a single mission in the bakcground and log the progress
target=/data/data_l0_pyglider/complete_mission/SEA$1/M$2*
rm -rf $target
nohup /data/miniconda/envs/process/bin/python /home/pipeline/utility_scripts/pyglider_single_mission.py $1 $2 raw > /data/log/complete_mission/error_complete_SEA$1_$2.log 2>&1 &

