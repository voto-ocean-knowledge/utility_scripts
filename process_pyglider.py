import os
import sys
import pathlib
import shutil
import yaml
from geocode import get_seas_merged_nav_nc

script_dir = pathlib.Path(__file__).parent.absolute()
parent_dir = script_dir.parents[0]
pyglider_dir = parent_dir / 'pyglider'
sys.path.append(str(pyglider_dir))
os.chdir(pyglider_dir)

import pyglider.seaexplorer as seaexplorer
import pyglider.ncprocess as ncprocess


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
    original_deploymentyaml = f'/data/deployment_yaml/mission_yaml/SEA{str(glider)}_M{str(mission)}.yml'
    deploymentyaml = f"/data/tmp/deployment_yml/SEA{str(glider)}_M{str(mission)}.yml"

    if not steps:
        steps = [1, 1, 1, 1]
    if len(steps) != 4:
        raise ValueError('steps must have exactly four items')
    if steps[0]:
        # clean last processing...
        safe_delete([rawncdir, l0tsdir, profiledir, griddir])
    if steps[1]:
        # turn seaexplorer zipped csvs into nc files.
        seaexplorer.raw_to_rawnc(rawdir, rawncdir, original_deploymentyaml, incremental=True)
    if steps[2]:
        # merge individual netcdf files into single netcdf files *.gli*.nc and *.pld1*.nc
        seaexplorer.merge_rawnc(rawncdir, rawncdir, original_deploymentyaml, kind=kind)
        # geolocate and add helcom basin info to yaml
        with open(original_deploymentyaml) as fin:
            deployment = yaml.safe_load(fin)
        nav_nc = list(pathlib.Path(rawncdir).glob("*rawgli.nc"))[0]
        basin = get_seas_merged_nav_nc(nav_nc)
        deployment['metadata']["basin"] = basin
        with open(deploymentyaml, "w") as fin:
            yaml.dump(deployment, fin)
    if steps[3]:
        # Make level-0 timeseries netcdf file from the raw files...
        outname = seaexplorer.raw_to_L0timeseries(rawncdir, l0tsdir, deploymentyaml, kind=kind)
        ncprocess.make_L0_gridfiles(outname, griddir, deploymentyaml)
        ncprocess.extract_L0timeseries_profiles(outname, profiledir, deploymentyaml)


if __name__ == '__main__':
    proc_pyglider_l0(63, 35, "sub", "/data/data_raw/nrt/SEA063/000035/C-Csv", "/data/tmp/out")
