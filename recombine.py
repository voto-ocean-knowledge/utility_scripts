import pathlib
import argparse
import sys
import os
import glob
import xarray as xr
import numpy as np
import logging


script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)
from pyglider_single_mission import fix_profile_number

_log = logging.getLogger(__name__)

def recombine(glider_num, mission_num):
    sub_dirs = list(pathlib.Path(f"/data/data_l0_pyglider/complete_mission/SEA{glider_num}").glob(f"M{mission_num}_sub*"))
    _log.info(f"found {len(sub_dirs)} sub dirs to recombine")
    sub_dirs.sort()
    sub_timeseries = []
    sub_gridfiles = []
    for out_sub_dir in sub_dirs:
        sub_times = glob.glob(f"{out_sub_dir}/timeseries/*.nc")
        if sub_times:
            sub_timeseries.append(sub_times[0])
        else:
            _log.info(f"No timeseries file in {out_sub_dir}")
        sub_grid = glob.glob(f"{out_sub_dir}/gridfiles/*.nc")
        if sub_grid:
            sub_gridfiles.append(sub_grid[0])
        else:
            _log.info(f"No grid file in {out_sub_dir}")

    mission_timeseries = xr.open_mfdataset(sub_timeseries, combine='by_coords', decode_times=False)
    _log.info('loaded timeseries')
    mission_timeseries = fix_profile_number(mission_timeseries, var_name='profile_index')
    _log.info('fixed timeseries profile numbers')
    mission_timeseries.to_netcdf(f"/data/data_l0_pyglider/complete_mission/SEA{glider_num}/M{mission_num}/timeseries/mission_timeseries.nc")
    _log.info('wrote mission timeseries')
    # free up memory
    mission_timeseries = None

    mission_grid = xr.open_mfdataset(sub_gridfiles, combine='by_coords', decode_times=False)
    _log.info('loaded gridded')
    mission_grid = fix_profile_number(mission_grid)
    _log.info('fixed gridded profile numbers')
    mission_grid.to_netcdf(f"/data/data_l0_pyglider/complete_mission/SEA{glider_num}/M{mission_num}/gridfiles/mission_grid.nc")
    _log.info('wrote gridded')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='recombine SX files with pyglider')
    parser.add_argument('glider', type=int, help='glider number, e.g. 70')
    parser.add_argument('mission', type=int, help='Mission number, e.g. 23')

    args = parser.parse_args()

    logf = f'/data/log/complete_mission/recombine_SEA{str(args.glider)}_M{str(args.mission)}.log'
    logging.basicConfig(filename=logf,
                        filemode='w',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    recombine(args.glider, args.mission)

