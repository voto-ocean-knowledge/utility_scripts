from pathlib import Path
from office_check_glider_files import list_missions, skip_projects, secrets, erddap_download, explained_missions, good_mission, adcp_proc_check
from ad2cp_nc_clean import proc

base = Path(secrets["data_path"])


if __name__ == '__main__':
    mission_list = list_missions(to_skip=skip_projects)
    processed_missions = erddap_download()
    for mission in mission_list:
        proc(mission, reprocess=False)
        good_mission(mission, processed_missions, explained=explained_missions)
        adcp_proc_check(mission)
        