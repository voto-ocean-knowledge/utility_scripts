import pathlib
import argparse
import sys
import os
import glob
import xarray as xr
import numpy as np
import logging
import post_process_dataset
from utilities import encode_times, set_best_dtype
from geocode import locs_to_seas

script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)

_log = logging.getLogger(__name__)


def recombine(glider_num, mission_num):
    sub_dirs = list(pathlib.Path(f"/data/tmp/subs").glob(f"proc_SEA{glider_num}_M{mission_num}_sub_*"))
    _log.info(f"Recombining glider {glider_num} mission {mission_num} from {len(sub_dirs)} batches")
    output_dir = f"/data/data_l0_pyglider/complete_mission/SEA{glider_num}/M{mission_num}/"
    if not pathlib.Path(output_dir).exists():
        pathlib.Path(output_dir).mkdir(parents=True)
    rawncdir = output_dir + 'rawnc/'
    l0tsdir = output_dir + 'timeseries/'
    griddir = output_dir + 'gridfiles/'
    for dir in [rawncdir, l0tsdir, griddir]:
        if not pathlib.Path(dir).exists():
            pathlib.Path(dir).mkdir(parents=True)
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
    try:
        mission_timeseries = xr.open_mfdataset(sub_timeseries, combine='by_coords', decode_times=False)
        encode = False
    except ValueError:
        encode = True
        _log.warning("mission timeseries combine mf failed. Trying manual")
        mission_timeseries = xr.open_dataset(sub_timeseries[0])
        for add_nc in sub_timeseries[1:]:
            add_ds = xr.open_dataset(add_nc)
            mission_timeseries = xr.concat((mission_timeseries, add_ds), dim="time")
            add_ds.close()
        mission_timeseries = mission_timeseries.sortby("time")
    _log.info('loaded timeseries')
    total_dives = len(np.unique(mission_timeseries.dive_num.values))
    mission_timeseries.attrs["total_dives"] = total_dives
    basin = locs_to_seas(mission_timeseries["longitude"].values[::10000], mission_timeseries["latitude"].values[::10000])
    mission_timeseries.attrs["basin"] = basin
    mission_timeseries = post_process_dataset.post_process(mission_timeseries)
    if encode:
        mission_timeseries = encode_times(mission_timeseries)
    mission_timeseries = set_best_dtype(mission_timeseries)
    mission_timeseries.to_netcdf(l0tsdir + "mission_timeseries.nc")
    _log.info('wrote mission timeseries')
    # free up memory
    mission_timeseries.close()

    mission_grid = xr.open_mfdataset(sub_gridfiles, combine='by_coords', decode_times=False)
    mission_grid.attrs["basin"] = basin
    _log.info('loaded gridded')
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
