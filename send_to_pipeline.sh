#!/bin/bash
# Utility to rsync completed mission data to ERDDAP server
glider=$1
mission=$2
echo send SEA$glider mission $mission data to pipeline
tgtdir=/media/data/data_dir/complete_mission/SEA$glider/M$mission/timeseries
echo make directory on target if it does not already exist
ssh erddapdata@13.51.101.57 mkdir -p $tgtdir
echo ""
echo rsync data
rsync -v --stats /data/data_l0_pyglider/complete_mission/SEA$glider/M$mission/timeseries/mission_timeseries.nc  "erddapdata@13.51.101.57:$tgtdir"
echo Finished