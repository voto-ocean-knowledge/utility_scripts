#!/bin/bash
# Utility to clean up sub folders after processing
glider=$1
mission=$2
echo clean SEA$glider mission $mission data
missionsub="proc_SEA${glider}_M${mission}_sub"
echo ""
echo raw sub data
ls -d1 /data/tmp/subs/raw_$missionsub*


echo ""
echo processed sub data
ls /data/tmp/subs/proc_$missionsub*
echo ""
echo removing sub data
echo ""
rm -rf /data/tmp/subs/raw_$missionsub*
rm -rf /data/tmp/subs/proc_$missionsub*
echo ""
echo removing rawnc files
rm -rf /data/data_l0_pyglider/complete_mission/SEA$glider/M$mission/rawnc