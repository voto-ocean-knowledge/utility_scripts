import os
import sys
import pathlib
import logging
import pandas as pd

script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)
from process_pyglider import proc_pyglider_l0

_log = logging.getLogger(__name__)
logging.basicConfig(filename='/data/log/pyglider_nrt.log',
                    filemode='a',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


def proc_nrt():
    _log.info("Start nrt processing")
    try:
        to_process = pd.read_csv('/home/pipeline/to_process.csv', dtype=int)
    except FileNotFoundError:
        _log.error("/home/pipeline/to_process.csv not found")
        return
    for i, row in to_process.iterrows():
        glider = str(row.glider)
        mission = str(row.mission)
        input_dir = f"/data/data_raw/nrt/SEA{glider.zfill(3)}/{mission.zfill(6)}/C-Csv/"
        output_dir = f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/"
        gridfiles_dir = f"{output_dir}gridfiles/"
        proc_steps = (0, 1, 1, 1)
        try:
            nc_file = list(pathlib.Path(gridfiles_dir).glob('*.nc'))[0]
            nc_time = nc_file.lstat().st_mtime
        except IndexError:
            _log.warning(f"no nc file found int {gridfiles_dir}. Reprocessing all data")
            nc_time = 0
            proc_steps = (1, 1, 1, 1)
        infile_time = 1
        in_files = list(pathlib.Path(input_dir).glob('*'))
        for file in in_files:
            if file.lstat().st_mtime > infile_time:
                infile_time = file.lstat().st_mtime
        if nc_time > infile_time:
            _log.info(f"No new SEA{glider} M{mission} input files")
            continue
        _log.info(f"Processing SEA{glider} M{mission}")
        proc_pyglider_l0(glider, mission, 'sub', input_dir, output_dir, steps=proc_steps)
    _log.info("Finished nrt processing")


if __name__ == '__main__':
    proc_nrt()
