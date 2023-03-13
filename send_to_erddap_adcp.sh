#!/bin/bash
# Utility to rsync completed mission data to ERDDAP server
glider=$1
mission=$2
echo send SEA$glider mission $mission data to ERDDAP
tgtdir=/media/data/complete_mission/SEA$glider/M$mission/ADCP
echo make directory on target if it does not already exist
ssh usrerddap@13.51.101.57 mkdir -p $tgtdir
echo ""
echo rsync data
rsync -v  /data/data_l0_pyglider/complete_mission/SEA$glider/M$mission/ADCP/adcp.nc  "usrerddap@13.51.101.57:$tgtdir"
echo Finished