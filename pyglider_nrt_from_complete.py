from pathlib import Path
import numpy as np
import xarray as xr
import logging
import argparse
_log = logging.getLogger(__name__)
logging.basicConfig(filename='/data/log/nrt_from_complete.log',
                    filemode='a',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


def nrt_proc_from_complete_nc(glider, mission):
    nc = Path(f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/timeseries/mission_timeseries.nc")
    ds = xr.open_dataset(nc)
    dives = ds.dive_num.values
    sample_num = np.arange(len(dives))
    keep_array = np.empty(len(dives), dtype=bool)
    keep_array[:] = False
    keep_array[np.logical_and(dives % 10 == 0, sample_num % 40 == 0)] = True

    ds_new = xr.Dataset()
    time = ds.time.values[keep_array]
    ds_new["time"] = time

    for coordinate in ds.coords:
        ds_new[coordinate] = ('time', ds.coords[coordinate].values[keep_array], ds.coords[coordinate].attrs)
    for var in list(ds):
        ds_new[var] = ('time', ds[var].values[keep_array], ds[var].attrs)

    ds_new = ds_new.assign_coords({"longitude": ds_new.longitude, "latitude": ds_new.latitude, "depth": ds_new.depth})
    ds_new.attrs = ds.attrs
    ds.attrs["total_dives"] = np.unique(ds_new.dive_num.values)
    out_path = Path(f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/timeseries")
    if not out_path.exists():
        out_path.mkdir(parents=True)
        
    if 'units' in ds_new.time.attrs.keys():
        ds_new.time.attrs.pop('units')
    if 'calendar' in ds_new.time.attrs.keys():
        ds_new.time.attrs.pop('calendar')
    if 'ad2cp_time' in list(ds_new):
        if 'units' in ds_new.ad2cp_time.attrs.keys():
            ds_new.ad2cp_time.attrs.pop('units')

    ds_new.to_netcdf(out_path / "mission_timeseries.nc", encoding={'time': {'units':'seconds since 1970-01-01T00:00:00Z'}})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='process SX files with pyglider')
    parser.add_argument('glider', type=int, help='glider number, e.g. 70')
    parser.add_argument('mission', type=int, help='Mission number, e.g. 23')

    args = parser.parse_args()
    _log.info(f"Processing SEA{args.glider} M{args.mission}")
    nrt_proc_from_complete_nc(args.glider, args.mission)
    _log.info(f"Processed SEA{args.glider} M{args.mission}")
