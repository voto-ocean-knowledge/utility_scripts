#!/bin/bash
# Utility to rsync raw mission data to pipeline
glider=$1
mission=$2
echo send SEA$glider mission $mission data to pipeline
tgtdir=/data/data_raw/complete_mission/SEA$glider/M$mission
echo make directory on target if it does not already exist
ssh pipeline@16.170.107.21 mkdir -p $tgtdir
echo ""
echo rsync data
rsync -v --stats /home/callum/Downloads/upload/*/sea*$glider.$mission* "pipeline@16.170.107.21:$tgtdir"
echo Finished
