from pathlib import Path
import pandas as pd
from itertools import chain
from ad2cp_file_move import adcp_proc_check

explained_missions = ((67, 15), (61, 63), (56, 27), (66, 31), (45, 58), (61, 48), (45, 37), (45, 54), (44, 48),
                      (55, 16), (63, 40), (66, 45))
skip_projects = ["1_Folder_Template", "2_Simulations", "3_SAT_Missions", "10_Oman_001", "8_KAMI-KZ_001",
                 "11_Amundsen_Sea"]


def erddap_download():
    datasets_url = "https://erddap.observations.voiceoftheocean.org/erddap/tabledap/allDatasets.csv"
    df_erddap = pd.read_csv(datasets_url)
    df_erddap.drop(0, inplace=True)
    complete_glider_missions = df_erddap[df_erddap.datasetID.str[:7] == "delayed"].datasetID
    erddap_missions = []
    for mission_str in complete_glider_missions:
        __, glider_str, mission_str = mission_str.split("_")
        glider = int(glider_str[3:])
        mission = int(mission_str[1:])
        erddap_missions.append((glider, mission))
    return erddap_missions


def good_mission(download_mission_path, processed_missions, explained=()):
    parts = list(download_mission_path.parts)
    parts[7] = "3_Non_Processed"
    mission_path = Path(*parts)
    pretty_mission = str(mission_path)[85:]
    glidermission = mission_path.parts[-1]
    try:
        glider_str, mission_str = glidermission.split("_")
        glider = int(glider_str[3:])
        mission = int(mission_str[1:])
    except ValueError:
        print(f"Could not proc {pretty_mission}")
        return
    if (glider, mission) in explained:
        print(f"known bad mission {glider, mission}. Skipping")
        return
    if not mission_path.is_dir():
        print(f"Downloaded but not processed {pretty_mission}")
    pld_path = mission_path / "PLD_raw"
    if not pld_path.is_dir():
        print(f"no pld, {pretty_mission}")
    nav_path = mission_path / "NAV"
    if not nav_path.is_dir():
        print(f"no nav, {pretty_mission}")
    pld_files = list(pld_path.glob(f"sea{str(glider).zfill(3)}.{mission}.pld1.raw*"))
    nav_files = list(nav_path.glob(f"sea{str(glider).zfill(3)}.{mission}.gli.sub*"))
    if len(pld_files) == 0 or len(nav_files) == 0:
        print(f"No matching files {pretty_mission} ")
    missmatch = abs(len(pld_files) - len(nav_files))
    if missmatch > 50:
        print(f"Missmatch {len(nav_files)} nav files vs {len(pld_files)} pld files {pretty_mission}")

    if (glider, mission) not in processed_missions:
        print(f"Not processed {pretty_mission}")


def list_missions(to_skip=()):
    base = Path("/run/user/1000/gvfs/afp-volume:host=VOTO_Storage.local,user=callum.rollo,volume=DATA/")
    projects = list(base.glob("*_*"))
    glider_dirs = []
    for proj in projects:
        good = True
        str_proj = str(proj)
        for skip in to_skip:
            if skip in str_proj:
                print(f"skipping {skip}")
                good = False
        if not good:
            continue
        non_proc = proj / "1_Downloaded"
        if non_proc.is_dir:
            proj_glider_dirs = non_proc.glob("SEA*")
            glider_dirs.append(list(proj_glider_dirs))
    glider_dirs = list(chain(*glider_dirs))

    all_mission_paths = []
    for glider_dir in glider_dirs:
        mission_dirs = list(glider_dir.glob("SEA*"))
        all_mission_paths.append(mission_dirs)
    all_mission_paths = list(chain(*all_mission_paths))
    return all_mission_paths


if __name__ == '__main__':


    mission_paths = list_missions(to_skip=skip_projects)
    processed_missions = erddap_download()
    for mission_dir in mission_paths:
        good_mission(mission_dir, processed_missions, explained=explained_missions)
        adcp_proc_check(mission_dir)