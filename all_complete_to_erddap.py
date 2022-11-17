import os
import sys
import pathlib
import pandas as pd
import logging
import subprocess

script_dir = pathlib.Path(__file__).parent.absolute()
parent_dir = script_dir.parents[0]
sys.path.append(str(script_dir))
os.chdir(script_dir)

_log = logging.getLogger(__name__)


if __name__ == '__main__':
    logf = f'/data/log/complete_mission_to_erddap.log'
    logging.basicConfig(filename=logf,
                        filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    _log.info("Start send to erddap")
    df_reprocess = pd.read_csv('/home/pipeline/reprocess.csv', parse_dates=["proc_time"])
    df_reprocess.sort_values("proc_time", inplace=True)
    total = len(df_reprocess)
    _log.info(f"will send {total} files to erddap")
    for i, row in df_reprocess.iterrows():
        glider, mission, proc = row.glider, row.mission, row.proc_time
        _log.info(f"Send file {i}/{total}: SEA{glider} M{mission}")
        subprocess.check_call(['/usr/bin/bash', "/home/pipeline/utility_scripts/send_to_pipeline.sh", str(glider), str(mission)])
    _log.info(f"Complete send to erddap")
