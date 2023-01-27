import xarray as xr
from pathlib import Path
import subprocess
from office_check_glider_files import list_missions, skip_projects

base = Path("/run/user/1000/gvfs/afp-volume:host=VOTO_Storage.local,user=callum.rollo,volume=DATA/")


def proc(mission_dir, reprocess=False):
    dir_parts = list(mission_dir.parts)
    dir_parts[7] = "4_Processed"
    adcp_dir = Path(*dir_parts) / "ADCP"
    if not adcp_dir.exists():
        # TODO check if this is a mission with ADCP data or not
        #print(f"no adcp data for {pretty_mission}")
        return
    pretty_mission = str(mission_dir)[85:]
    glider_str, mission_str = dir_parts[-1].split("_")
    glider = int(glider_str[3:])
    mission = int(mission_str[1:])
    fn = f"sea{glider}_m{mission}.ad2cp.00000.nc"
    nc = adcp_dir / fn
    insize = nc.lstat().st_size
    fout = adcp_dir / f"sea{glider}_m{mission}_ad2cp.nc"
    if fout.exists():
        outsize = fout.lstat().st_size
        if outsize == 0:
            print(f"oh no {pretty_mission}")
            return
        if insize / outsize > 10:
            print(f"warning: resultant file more than 10x smaller {pretty_mission}")
        if not reprocess:
            return
    print(f"opening {pretty_mission} {fn}")
    config = xr.open_dataset(nc, group="Config").attrs
    data = xr.open_dataset(nc, group="Data/Average")
    # TODO decide on using bottom track data or not
    #data_btrack = xr.open_dataset(nc, group="Data/AverageBT")
    attrs = {}
    skip_attrs = ["fileName", "fileName_description", "File_file_directory", "File_file_directory_description"]
    for i, (key, val) in enumerate(config.items()):
        if key in skip_attrs:
            continue
        #"rawConfiguration" causes problems. HD5 doesn't allow attributes over 64k, which is 4096 chars, so split long strings up
        if type(val) is str and len(val) > 4000:
            attrs[key] = val[:4000]
            attrs[f"{key}_continued"] = val[4000:]
            continue
        if key == "rawConfiguration":
            val = val[:4096]
            #continue
        attrs[key] = val
    data.attrs = attrs
    print(f"writing {fout}")
    data.to_netcdf(fout)
    print("send to pipeline")
    subprocess.check_call(
        ['/usr/bin/bash', "/home/callum/Documents/data-flow/raw-to-nc/utility_scripts/upload_adcp.sh",
         str(glider), str(mission), str(fout)])
    print("finished")


def main():
    mission_list = list_missions(to_skip=skip_projects)
    skip_list =["/run/user/1000/gvfs/afp-volume:host=VOTO_Storage.local,user=callum.rollo,volume=DATA/6_SAMBA_003/1_Downloaded/SEA063_PLD089/SEA063_M24",
                "/run/user/1000/gvfs/afp-volume:host=VOTO_Storage.local,user=callum.rollo,volume=DATA/6_SAMBA_003/1_Downloaded/SEA045_PLD069/SEA045_M36",
                "/run/user/1000/gvfs/afp-volume:host=VOTO_Storage.local,user=callum.rollo,volume=DATA/7_SAMBA_004/1_Downloaded/SEA056_PLD070/SEA056_M40",]
    print(f"Failing and skipping these missions: {skip_list}")
    for mission in mission_list:
        if str(mission) in skip_list:
            continue
        proc(mission, reprocess=True)


if __name__ == '__main__':
    main()
