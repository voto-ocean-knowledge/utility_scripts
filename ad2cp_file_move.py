from pathlib import Path
import shutil
from office_check_glider_files import list_missions, skip_projects


def clean_adcp_fn(fn):
    fn = fn.lower()
    name, ext = fn.split(".")
    glider, mission = name.split("_")
    glider_fix = f"{glider[:3]}{str(int(glider[3:]))}"
    mission_fix = f"m{str(int(mission[1:]))}"
    return f"{glider_fix}_{mission_fix}.{ext}"


def adcp_proc_check(download_mission_path):
    parts = list(download_mission_path.parts)
    parts[7] = "3_Non_Processed"
    mission_path = Path(*parts)
    pretty_mission = str(mission_path)[85:]

    glidermission = mission_path.parts[-1]
    adcp_dir = mission_path / "ADCP"
    if not adcp_dir.exists():
        return
    try:
        all_adcp_files = list(adcp_dir.glob("*.ad2cp"))
        if len(all_adcp_files) > 1:
            print(f"multiple .adcp files in {pretty_mission}")
            return
        adcp_file = all_adcp_files[0]
    except:
        print(f"no .ad2cp file found in {pretty_mission}")
        return
    adcp_parts = list(adcp_file.parts)
    adcp_parts[7] = "4_Processed"
    adcp_clean_fn = clean_adcp_fn(adcp_parts[-1])
    adcp_parts[-1] = adcp_clean_fn
    ad2cp_path_clean = Path(*adcp_parts)

    ad2cp_dir_clean = Path(*adcp_parts[:-1])
    pretty_mission_proc = str(ad2cp_dir_clean)[85:]
    adcp_proc_dir = Path(*adcp_parts[:-6]) / "temprary_data_store" / "adcp_proc"
    adcp_proc_path = adcp_proc_dir / adcp_clean_fn
    if not adcp_proc_path.exists():
        shutil.copy(adcp_file, adcp_proc_path)
    if not ad2cp_dir_clean.exists():
        ad2cp_dir_clean.mkdir(parents=True)

    adcp_proc_4_fn = ad2cp_dir_clean / (adcp_clean_fn + ".00000.nc")
    if adcp_proc_4_fn.exists():
        return

    nc = adcp_proc_dir / (adcp_clean_fn + ".00000.nc")
    try:
        nc_path = list(ad2cp_dir_clean.glob("*.nc"))[0]
    except:
        print(f"no nc found in {pretty_mission_proc}")
        if nc.exists():
            print(f"copying nc file to {pretty_mission_proc}")
            shutil.copy(nc, adcp_proc_4_fn)

    return


def main():
    mission_list = list_missions(to_skip=skip_projects)
    for mission in mission_list:
        adcp_proc_check(mission)


if __name__ == '__main__':
    main()
