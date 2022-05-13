import geopandas as gp
import pandas as pd
import xarray as xr
import logging
import pathlib
import argparse
import shutil
from collections import Counter
_log = logging.getLogger(__name__)
from itertools import chain

df_helcom = gp.read_file("/data/third_party/helcom_basins/HELCOM_subbasins_2022.shp")


def get_seas(gridfile):
    ds = xr.open_dataset(gridfile)
    lon = ds.longitude
    lat = ds.latitude
    df_glider = pd.DataFrame({"lon": lon, "lat": lat})
    df_glider = gp.GeoDataFrame(df_glider, geometry=gp.points_from_xy(df_glider.lon, df_glider.lat))
    df_glider = df_glider.set_crs(epsg=4326)
    df_glider = df_glider.to_crs(df_helcom.crs)
    polygons_contains = gp.sjoin(df_helcom, df_glider, op='contains')
    basin_points = polygons_contains.level_2.values
    if not basin_points:
        return ""
    basin_counts = Counter(basin_points).most_common()
    basins_ordered = [x[0] for x in basin_counts]
    basin_str = ", ".join(basins_ordered)
    return basin_str


def nc_add_sea(nc_path, basin_str, tempfile):
    _log.info(f"working on {nc_path}")
    ds = xr.open_dataset(nc_path)
    _log.info('read files successfully')
    meta = ds.attrs
    meta["basin"] = basin_str
    ds.attrs = meta
    ds.to_netcdf(tempfile)
    shutil.move(tempfile, nc_path)
    _log.info("Successfully saved nc")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='add sub basin data to netcdf files')
    parser.add_argument('glider', type=int, help='glider number, e.g. 70')
    parser.add_argument('mission', type=int, help='Mission number, e.g. 23')
    args = parser.parse_args()
    if args.kind not in ['raw', 'sub', None]:
        raise ValueError('kind must be raw or sub')
    logf = f'/data/log/update_meta/SEA{str(args.glider)}_M{str(args.mission)}.log'
    logging.basicConfig(filename=logf,
                        filemode='w',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    types = []
    if args.kind in ['sub', None]:
        sub_dir = "nrt"
    else:
        sub_dir = 'complete_mission'
        
    root_dir = f"/data/data_l0_pyglider/{sub_dir}/SEA{str(args.glider)}/M{str(args.mission)}"
    _log.info(f"add basin to ncs in {root_dir}")
    nc_files = []
    for sub in ('profiles', 'timeseries', 'gridfiles'):
        nc_files.append(list(pathlib.Path(root_dir).glob(f"**/{sub}/*.nc")))
    nc_files_flat = list(chain.from_iterable(nc_files))
    if not nc_files:
        _log.error(f"no ncs found in path {root_dir}")
    _log.info(f"Found {len(nc_files_flat)}")
    gridfile_dir = pathlib.Path(f"/data/data_l0_pyglider/{sub_dir}/SEA{str(args.glider)}/M{str(args.mission)}/gridfiles")
    gridfile = list(gridfile_dir.glob("*.nc"))[0]
    basin = get_seas(gridfile)
    _log.info(f"Basin: {basin}")
    for nc in nc_files_flat:
        temp_nc = pathlib.Path("/tmp") / pathlib.Path(nc).name
        nc_add_sea(nc, basin, temp_nc)
    _log.info(f"Updated all ncs in {root_dir}")
    _log.info("Success! added basin to all ncs")
