import os
import sys
import pathlib
import shutil
import pandas as pd
import logging

script_dir = pathlib.Path(__file__).parent.absolute()
parent_dir = script_dir.parents[0]
pyglider_dir = parent_dir / 'pyglider'
sys.path.append(str(pyglider_dir))
os.chdir(pyglider_dir)

import pyglider.seaexplorer as seaexplorer
import pyglider.ncprocess as ncprocess

_log = logging.getLogger(__name__)
logging.basicConfig(filename='/data/log/pyglider_nrt.log',
                    filemode='a',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


def safe_delete(directories):
    for directory in directories:
        if pathlib.Path.exists(pathlib.Path.absolute(script_dir / directory)):
            shutil.rmtree(directory)


def proc_pyglider_l0(glider, mission, kind, input_dir, output_dir, steps=()):
    if kind not in ['raw', 'sub']:
        raise ValueError('kind must be raw or sub')
    rawdir = input_dir + '/'
    output_path = pathlib.Path(output_dir)
    if not output_path.exists():
        output_path.mkdir(parents=True)
    rawncdir = output_dir + 'rawnc/'
    l0tsdir = output_dir + 'timeseries/'
    profiledir = output_dir + 'profiles/'
    griddir = output_dir + 'gridfiles/'
    deploymentyaml = f'/data/deployment_yaml/mission_yaml/SEA{str(glider)}_M{str(mission)}.yml'
    if not steps:
        steps = [1, 1, 1, 1]
    if len(steps) != 4:
        raise ValueError('steps must have exactly four items')
    if steps[0]:
        # clean last processing...
        safe_delete([rawncdir, l0tsdir, profiledir, griddir])
    if steps[1]:
        # turn seaexplorer zipped csvs into nc files.
        seaexplorer.raw_to_rawnc(rawdir, rawncdir, deploymentyaml, incremental=True)
    if steps[2]:
        # merge individual netcdf files into single netcdf files *.gli*.nc and *.pld1*.nc
        seaexplorer.merge_rawnc(rawncdir, rawncdir, deploymentyaml, kind=kind)
    if steps[3]:
        # Make level-0 timeseries netcdf file from the raw files...
        outname = seaexplorer.raw_to_L0timeseries(rawncdir, l0tsdir, deploymentyaml, kind=kind)
        ncprocess.make_L0_gridfiles(outname, griddir, deploymentyaml)
        ncprocess.extract_L0timeseries_profiles(outname, profiledir, deploymentyaml)


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
