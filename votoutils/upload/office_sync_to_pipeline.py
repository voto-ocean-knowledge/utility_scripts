from pathlib import Path
from votoutils.monitor.office_check_glider_files import list_missions, skip_projects, secrets, erddap_download, explained_missions, good_mission, adcp_proc_check
from votoutils.ad2cp.ad2cp_nc_clean import proc
from votoutils.utilities.utilities import mailer
import logging
_log = logging.getLogger(__name__)
base = Path(secrets["data_path"])


if __name__ == '__main__':
    logf = f'/data/log/office_sync_to_pipeline.log'
    logging.basicConfig(filename=logf,
                        filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    _log.info(f"start check of office fileserver for new complete missions")
    mission_list = list_missions(to_skip=skip_projects)
    processed_missions = erddap_download()
    for mission in mission_list:
        try:
            proc(mission, reprocess=False)
        except:
            mailer("adcp-proc-error", f"failed with {mission}")
        good_mission(mission, processed_missions, explained=explained_missions)
        adcp_proc_check(mission)
    _log.info(f"complete")
