#!/bin/bash
# Utility to rsync raw mission data to pipeline
glider=$1
mission=$2
adcpfile=$3
echo send SEA$glider mission $mission adcp data to pipeline
tgtdir=/data/data_raw/complete_mission/SEA$glider/M$mission/ADCP
echo make directory on target if it does not already exist
ssh pipeline@16.170.107.21 mkdir -p $tgtdir
echo ""
echo rsync data
rsync $adcpfile "pipeline@88.99.244.110:$tgtdir"
echo Finished
