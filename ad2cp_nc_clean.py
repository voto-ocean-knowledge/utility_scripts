import xarray as xr
from pathlib import Path
from office_check_glider_files import list_missions, skip_projects

base = Path("/run/user/1000/gvfs/afp-volume:host=VOTO_Storage.local,user=callum.rollo,volume=DATA/")



def proc(mission_dir):
    # TODO check if this is a mission with ADCP data or not
    dir_parts = list(mission_dir.parts)
    dir_parts[7] = "4_Processed"
    adcp_dir = Path(*dir_parts) / "ADCP"
    pretty_mission = str(mission_dir)[85:]
    glider_str, mission_str = dir_parts[-1].split("_")
    glider = int(glider_str[3:])
    mission = int(mission_str[1:])
    if not adcp_dir.exists():
        print(f"no adcp data for {pretty_mission}")
        return
    fn = f"sea{glider}_m{mission}.ad2cp.00000.nc"
    nc = adcp_dir / fn
    config = xr.open_dataset(nc, group="Config").attrs
    data = xr.open_dataset(nc, group="Data/Average")
    #data_btrack = xr.open_dataset(nc, group="Data/AverageBT")
    attrs = {}
    skip_attrs = ["fileName", "fileName_description", "File_file_directory", "File_file_directory_description"]
    for i, (key, val) in enumerate(config.items()):
        if key in skip_attrs:
            continue
        if type(val) is str:
            # clean these chars out of strings or it won't write to netCDF
            val = val.replace('"', '')
        attrs[key] = val
    data.attrs = attrs
    fout = f"sea{glider}_m{mission}_ad2cp.nc"
    print(f"writing {fout}")
    data.to_netcdf(adcp_dir / fout)


if __name__ == '__main__':
    mission_list = list_missions(to_skip=skip_projects)
    for mission in mission_list:
        proc(mission)
