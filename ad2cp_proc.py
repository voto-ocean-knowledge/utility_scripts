import xarray as xr
import numpy as np
from utilities import encode_times

def proc_ad2cp_mission(nc, adcp_nc):
    # open datasets
    ts = xr.open_dataset(nc)
    adcp = xr.open_dataset(adcp_nc)
    # All adcp variables with a constant value converted to attributes
    for var_name in list(adcp):
        var = adcp[var_name].values
        uniques = np.unique(var)
        if len(uniques) == 1:
            adcp.attrs[var_name] = uniques[0]
            adcp = adcp.drop_vars(var_name)
    # test that beams stay the same throughout the dataset
    unique_beams = np.unique(adcp.Physicalbeam.values, axis=0)
    beam_attrs = adcp.Physicalbeam.attrs
    if unique_beams.shape[0] == 1:
        adcp.attrs["physical_beam"] = list(unique_beams[0])
        adcp = adcp.drop_vars("Physicalbeam")
    else:
        raise ValueError("physical beams are not consistent throughout mission! Abort")

    # align timestamps of adcp data to those of the glider
    ts_adcp_time = ts.ad2cp_time[~np.isnan(ts.ad2cp_time.values)]
    ts_time = ts.time[~np.isnan(ts.ad2cp_time.values)]
    adcp = adcp.reindex(time=ts_adcp_time, method="nearest")
    adcp = adcp.assign_coords(time=ts_time)
    # TODO tests here that the alignment of timestamps hasn't screwed things up
    # Drop the ad2cp data transmitted to the pld1 board during the mission
    print("aligned")
    for var_name in list(ts):
        if "ad2cp" in var_name:
            ts = ts.drop_vars(var_name)
    # Standardise the dimension names of the variables that are on a range
    for var_name in list(adcp):
        var = adcp[var_name]
        if "Range" in str(var.dims):
            var_fixed = var.rename({var.dims[1]: "range"})
            adcp = adcp.drop_vars(var_name)
            adcp[var_name] = var_fixed
    adcp = adcp.reset_index(['Amplitude Range', 'Correlation Range', 'Velocity Range'], drop=True)
    # Keep only 3D variables in the ad2cp dataset (time, cell, beam). All others transferred to the glider dataset
    print("move over 1d vars")
    for var_name in list(adcp):
        if "Beam" not in var_name:
            ts[f"ad2cp_{var_name}"] = adcp[var_name]
            adcp = adcp.drop_vars(var_name)

    # rearange the AD2CP data into 3D DataArrays. Using the native dtype of the data (32-bit float)
    print("pre-grid")
    dimensions = {"time": adcp.time, "cell": adcp.range, "beam": (1, 2, 3, 4)}

    for kind in ["Velocity", "Correlation", "Amplitude"]:
        vel = np.empty((*adcp[f"{kind}Beam1"].values.shape, 4), dtype=type(adcp[f"{kind}Beam1"].values[0, 0]))
        vel[:] = np.nan
        for beam in (1, 2, 3, 4):
            old_var = adcp[f"{kind}Beam{beam}"]
            vel[:, :, beam - 1] = old_var.values
            adcp = adcp.drop_vars(f"{kind}Beam{beam}")
        da_3d = xr.DataArray(data=vel, dims=dimensions)
        da_3d.attrs = old_var.attrs
        adcp[kind.lower()] = da_3d

    adcp.assign_coords(beam=("beam", np.array((1, 2, 3, 4))))
    adcp.beam.attrs = beam_attrs

    print("make ncs")
    adcp = encode_times(adcp)
    adcp.to_netcdf("adcp_3d.nc")
    print("make ts")
    ts = encode_times(ts)
    ts.to_netcdf("glider_with_adcp_1d.nc")


if __name__ == '__main__':
    proc_ad2cp_mission("/home/callum/Documents/data-flow/raw-to-nc/adcp/combine_with_timeseries/input/sea67_m39.nc",
                       "/home/callum/Documents/data-flow/raw-to-nc/adcp/combine_with_timeseries/input/sea67_m39_ad2cp.nc")