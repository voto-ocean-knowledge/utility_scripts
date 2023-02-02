# utility_scripts
Top level scripts to manage data processing

These scripts coordinate the processing of data from raw seaexplorer files to standardised, quality controlled, ERDDAP digestable netCDFs.

The most used scripts are:

- **ad2cp_** scripts process the Nortek AD2CP data files.
- **check_pipeline.py** Checks that log files end as expected, otherwise it sends an email to the data manager to investigate
- **process_pyglider** The main script for co-ordinating the processing of a dataset by [pyglider](https://github.com/c-proof/pyglider). Typically this is not run manually\
- **proc_single_mission.sh** is a utility script to process one complete mission from the command line
- **pyglider_nrt.py** Processed nrt data. This runs every 30 mins
- **pyglider_nrt_from_complete.py** creates artificial nrt datasets from delayed mode datasets that have no nrt data
