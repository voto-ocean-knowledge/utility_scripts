#!/bin/bash
# Utility to clean up sub folders after processing
glider=$1
mission=$2
echo clean SEA$glider mission $mission data
missionsub="${mission}_sub"
echo ""
echo raw sub data
ls -d1 /data/data_raw/complete_mission/SEA$glider/M$missionsub*


echo ""
echo processed sub data
ls /data/data_l0_pyglider/complete_mission/SEA$glider/M$missionsub*
echo ""
echo removing sub data
echo ""
rm -rf /data/data_raw/complete_mission/SEA$glider/M$missionsub*
rm -rf /data/data_l0_pyglider/complete_mission/SEA$glider/M$missionsub*
