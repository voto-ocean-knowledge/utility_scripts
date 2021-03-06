import pathlib
import argparse
import sys
import os
import glob
import xarray as xr
import shutil
import logging


script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)
from utilities import fix_profile_number

_log = logging.getLogger(__name__)


def recombine(glider_num, mission_num):
    sub_dirs = list(pathlib.Path(f"/data/data_l0_pyglider/complete_mission/SEA{glider_num}").glob(f"M{mission_num}_sub*"))
    _log.info(f"Recombining glider {glider_num} mission {mission_num} from {len(sub_dirs)} batches")
    output_dir = f"/data/data_l0_pyglider/complete_mission/SEA{glider_num}/M{mission_num}/"
    if not pathlib.Path(output_dir).exists():
        pathlib.Path(output_dir).mkdir(parents=True)
    rawncdir = output_dir + 'rawnc/'
    l0tsdir = output_dir + 'timeseries/'
    profiledir = output_dir + 'profiles/'
    griddir = output_dir + 'gridfiles/'
    for dir in [rawncdir, l0tsdir, profiledir, griddir]:
        if not pathlib.Path(dir).exists():
            pathlib.Path(dir).mkdir(parents=True)
    _log.info(f"Moving rawnc and l0 profiles files")
    for i in range(len(sub_dirs)):
        out_sub_dir = f"{output_dir[:-1]}_sub_{i}/"
        sub_rawncdir = out_sub_dir + 'rawnc/'
        sub_profiledir = out_sub_dir + 'profiles/'
        in_raw_nc = glob.glob(f"{sub_rawncdir}*.pld*.nc")
        for filename in in_raw_nc:
            shutil.move(filename, rawncdir)
        in_raw_gli = glob.glob(f"{sub_rawncdir}*.gli*.nc")
        for filename in in_raw_gli:
            shutil.move(filename, rawncdir)
        in_profile = glob.glob(f"{sub_profiledir}*.nc")
        for filename in in_profile:
            shutil.move(filename, profiledir)
    _log.info("Looking for timeseries and gridfiles to recombine")
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
    if not sub_timeseries or not sub_gridfiles:
        _log.info("no files to recombine. Stop")
        return
    mission_timeseries = xr.open_mfdataset(sub_timeseries, combine='by_coords', decode_times=False)
    _log.info('loaded timeseries')
    mission_timeseries = fix_profile_number(mission_timeseries, var_name='profile_index')
    _log.info('fixed timeseries profile numbers')
    mission_timeseries.to_netcdf(l0tsdir + "mission_timeseries.nc")
    _log.info('wrote mission timeseries')
    # free up memory
    mission_timeseries = None

    mission_grid = xr.open_mfdataset(sub_gridfiles, combine='by_coords', decode_times=False)
    _log.info('loaded gridded')
    mission_grid = fix_profile_number(mission_grid)
    _log.info('fixed gridded profile numbers')
    mission_grid.to_netcdf(griddir + "mission_grid.nc")
    _log.info('wrote gridded')
    _log.info("Recombination complete")


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

