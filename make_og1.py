import xarray as xr
from convert_to_og1 import convert_to_og1, standardise_og10
import subprocess

if __name__ == '__main__':
    ds = xr.open_dataset("/data/data_l0_pyglider/complete_mission/SEA76/M19/timeseries/mission_timeseries.nc")
    ds_standard = standardise_og10(ds)
    ds_og1 = convert_to_og1(ds_standard, num_vals=-1, postscript='delayed')
    outfile = f"/home/callum/Documents/community/OG-format-user-manual/og_format_examples_files/{ds_og1.attrs['id']}.nc"
    cdl = f"/home/callum/Documents/community/OG-format-user-manual/og_format_examples_files/{ds_og1.attrs['id']}.cdl"
    ds_og1.to_netcdf(outfile)
    my_cmd = ['ncdump', outfile]
    with open(cdl, "w") as outfile:
        subprocess.run(my_cmd, stdout=outfile)
