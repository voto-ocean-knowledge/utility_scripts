import os
import sys
import pathlib
import shutil
import yaml
import numpy as np
import polars as pl
import xarray as xr
from geocode import get_seas_merged_nav_nc

script_dir = pathlib.Path(__file__).parent.absolute()
parent_dir = script_dir.parents[0]
pyglider_dir = parent_dir / 'pyglider'
sys.path.append(str(pyglider_dir))
os.chdir(pyglider_dir)
qc_dir = parent_dir / "voto_glider_qc"
sys.path.append(str(qc_dir))
from flag_qartod import flagger, apply_flags

import pyglider.seaexplorer as seaexplorer
import pyglider.ncprocess as ncprocess


def safe_delete(directories):
    for directory in directories:
        if pathlib.Path.exists(pathlib.Path.absolute(script_dir / directory)):
            shutil.rmtree(directory)


def set_profile_numbers(ds, profile_bump=0):
    ds["dive_num"] = ds["dive_num"].astype(int)
    df = ds.to_pandas()
    df["profile_index"] = 0
    deepest_points = []
    dive_nums = np.unique(df.dive_num)
    for num in dive_nums:
        df_dive = df[df.dive_num == num]
        deep_inflect = df_dive[df_dive.pressure == df_dive.pressure.max()].index.values[0]
        deepest_points.append(deep_inflect)

    previous_deep_inflect = deepest_points[0]
    df.loc[df.index[0]:previous_deep_inflect, "profile_index"] = 0
    num = 0
    for i, deep_inflect in enumerate(deepest_points[1:]):
        num = i + 1
        df_deep_to_deep = df.loc[previous_deep_inflect: deep_inflect]
        shallow_inflect = df_deep_to_deep[df_deep_to_deep.pressure == df_deep_to_deep.pressure.min()].index.values[0]
        df.loc[previous_deep_inflect:shallow_inflect, "profile_index"] = num * 2 - 1
        df.loc[shallow_inflect:deep_inflect, "profile_index"] = num * 2
        previous_deep_inflect = deep_inflect
    df.loc[previous_deep_inflect:df.index[-1], "profile_index"] = num * 2 + 1

    df["profile_direction"] = -1
    df.loc[df.profile_index % 2 == 0, "profile_direction"] = 1
    ds["profile_index"] = df.dive_num.copy()
    ds["profile_direction"] = df.dive_num.copy()
    ds["profile_index"].values = df.profile_index + profile_bump
    ds["profile_direction"].values = df.profile_direction
    ds["profile_index"].attrs = {'long_name': 'profile index',
                                 'units': '1',
                                 'sources': 'pressure, time, dive_num'}
    ds["profile_direction"].attrs = {'long_name': 'profile direction',
                                     'units': '1',
                                     'sources': 'pressure, time, dive_num',
                                     'comment': '-1 = ascending, 1 = descending'}
    ds["profile"] = ds["profile_index"].copy()
    ds["profile"].attrs["long_name"] = "profile number"
    return ds


def proc_pyglider_l0(glider, mission, kind, input_dir, output_dir, steps=(), profile_bump=0):
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
        seaexplorer.merge_parquet(rawncdir, rawncdir, original_deploymentyaml, kind=kind)
        # geolocate and add helcom basin info to yaml
        with open(original_deploymentyaml) as fin:
            deployment = yaml.safe_load(fin)
        nav_nc = list(pathlib.Path(rawncdir).glob("*rawgli.parquet"))[0]
        basin = get_seas_merged_nav_nc(nav_nc)
        deployment['metadata']["basin"] = basin
        # More custom metadata
        df = pl.read_parquet(nav_nc)
        total_dives = df.select("fnum").unique().shape[0]
        deployment['metadata']["total_dives"] = total_dives
        glider_num_pad = str(deployment['metadata']['glider_serial']).zfill(3)
        dataset_type = "nrt" if kind == "sub" else "delayed"
        dataset_id = f"{dataset_type}_SEA{glider_num_pad}_M{deployment['metadata']['deployment_id']}"
        deployment['metadata']["dataset_id"] = dataset_id
        variables = list(deployment["netcdf_variables"].keys())
        if "keep_variables" in variables:
            variables.remove("keep_variables")
        if "timebase" in variables:
            variables.remove("timebase")
        deployment['metadata']["variables"] = variables
        with open(deploymentyaml, "w") as fin:
            yaml.dump(deployment, fin)
    if steps[3]:
        # Make level-0 timeseries netcdf file from the raw files...
        outname = seaexplorer.raw_to_L0timeseries(rawncdir, l0tsdir, deploymentyaml, kind=kind)
        tempfile = f"/data/tmp/SEA{glider}_M{mission}"
        int_vars = ["angular_cmd", "ballast_cmd", "linear_cmd", "nav_state", "security_level", "dive_num",
                    "desired_heading", "chlorophyll_raw", "phycocyanin_raw", "turbidity_raw", "backscatter_raw"]
        ds = xr.open_dataset(outname)
        ds = flagger(ds)
        ds_variables = list(ds)
        for var in ds_variables:
            if var in int_vars or var[-2:] == "qc":
                ds[var] = ds[var].astype(int)
        ds = set_profile_numbers(ds, profile_bump=profile_bump)
        max_profile = ds.profile_index.values.max()
        ds.to_netcdf(tempfile, encoding={'time': {'units': 'seconds since 1970-01-01T00:00:00Z'}})
        shutil.move(tempfile, outname)
        ds = apply_flags(ds)
        ds.to_netcdf(tempfile, encoding={'time': {'units': 'seconds since 1970-01-01T00:00:00Z'}})
        ncprocess.make_L0_gridfiles(tempfile, griddir, deploymentyaml)
        return max_profile
