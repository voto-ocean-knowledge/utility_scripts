#!/bin/bash
# Utility to display state of a glider's data in the pipeline. Requires glider serial number e.g. 63
glider=$1
echo SEA$glider data
echo ""
echo raw data
ls /data/data_raw/complete_mission/SEA$glider/
echo ""
ls -d1 /data/data_raw/complete_mission/SEA$glider/M??
echo ""
echo processed data
ls /data/data_l0_pyglider/complete_mission/SEA$glider/
echo ""
ls -1d /data/data_l0_pyglider/complete_mission/SEA$glider/M??
echo ""
echo plots
ls /data/plots/complete_mission/SEA$glider
