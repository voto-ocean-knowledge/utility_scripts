from pathlib import Path
import numpy as np
import xarray as xr
import logging
import argparse
from utilities import encode_times
_log = logging.getLogger(__name__)
logging.basicConfig(filename='/data/log/nrt_from_complete.log',
                    filemode='a',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


def nrt_proc_from_complete_nc(glider, mission):
    nc = Path(f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/timeseries/mission_timeseries.nc")
    ds = xr.open_dataset(nc)
    ds = ds.coarsen(time=40, boundary="trim").mean()
    dives = ds.profile_index.values
    keep_array = np.empty(len(dives), dtype=bool)
    keep_array[:] = True
    keep_array[dives % 20 > 1.1] = False
    keep_array[np.abs(ds.profile_direction) <= 0.5] = False
    ds_new = xr.Dataset()
    time = ds.time.values[keep_array]
    ds_new["time"] = time

    for coordinate in ds.coords:
        ds_new[coordinate] = ('time', ds.coords[coordinate].values[keep_array], ds.coords[coordinate].attrs)
    for var in list(ds):
        ds_new[var] = ('time', ds[var].values[keep_array], ds[var].attrs)

    ds_new = ds_new.assign_coords({"longitude": ds_new.longitude, "latitude": ds_new.latitude, "depth": ds_new.depth})
    ds_new.attrs = ds.attrs
    int_vars = ["angular_cmd", "ballast_cmd", "linear_cmd", "nav_state", "security_level", "dive_num",
                "desired_heading", "profile_direction", "profile_index", "profile_num"]
    ds_variables = list(ds_new)
    for var in ds_variables:
        if var in int_vars or var[-2:] == "qc":
            ds_new[var] = np.around(ds_new[var]).astype(int)
        elif var[-3:] == "raw":
            ds_new[var] = np.around(ds_new[var])

    ds_new.attrs["total_dives"] = len(np.unique(ds_new.dive_num.values))
    out_path = Path(f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/timeseries")
    if not out_path.exists():
        out_path.mkdir(parents=True)
    ds_new = encode_times(ds_new)
    ds_new.to_netcdf(out_path / "mission_timeseries.nc")
    foo = bar
    return


if __name__ == '__main__':
    nrt_proc_from_complete_nc(45, 43)
def hello():
    parser = argparse.ArgumentParser(description='process SX files with pyglider')
    parser.add_argument('glider', type=int, help='glider number, e.g. 70')
    parser.add_argument('mission', type=int, help='Mission number, e.g. 23')

    args = parser.parse_args()
    _log.info(f"Processing SEA{args.glider} M{args.mission}")
    nrt_proc_from_complete_nc(args.glider, args.mission)
    _log.info(f"Processed SEA{args.glider} M{args.mission}")
