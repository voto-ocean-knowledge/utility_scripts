from pathlib import Path
import numpy as np
import xarray as xr
import logging
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
    out_path = Path(f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/timeseries")
    if not out_path.exists():
        out_path.mkdir(parents=True)
    ds_new.to_netcdf(out_path / "mission_timeseries.nc")


def all_nrt_from_complete(reprocess=True):
    _log.info("Start nrt from complete")
    glider_paths = list(Path("/data/data_l0_pyglider/complete_mission").glob("SEA*"))
    glidermissions = []
    for glider_path in glider_paths:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            glider = int(glider_path.parts[-1][3:])
            mission = int(mission_path.parts[-1][1:])
            nrt_path = Path(f"/data/data_raw/nrt/SEA0{str(glider).zfill(2)}/{str(mission).zfill(6)}/C-Csv/")
            if nrt_path.exists():
                _log.debug(f"nrt path {nrt_path} exists. Skipping")
                continue
            try:
                glidermissions.append((int(glider_path.parts[-1][3:]), int(mission_path.parts[-1][1:])))
            except:
                _log.warning(f"Could not process {mission_path}")
    _log.info(f"will process {len(glidermissions)} missions")
    for glider,  mission in glidermissions:
        out_path = Path(f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/timeseries")
        if out_path.exists() and not reprocess:
            _log.info(f"SEA{glider} M{mission} already exists. Skipping")
            continue
        try:
            nrt_proc_from_complete_nc(glider, mission)
            _log.info(f"Processed SEA{glider} M{mission}")
        except:
            _log.warning(f"failed with SEA{glider} M{mission}")

    _log.info("Complete nrt from complete")


if __name__ == '__main__':
    all_nrt_from_complete()
