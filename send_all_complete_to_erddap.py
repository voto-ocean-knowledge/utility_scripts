import subprocess
import pathlib
import logging
_log = logging.getLogger(__name__)

logf = f'/data/log/send_all_to_erddap.log'
logging.basicConfig(filename=logf,
                    filemode='a',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


def send(glider, mission):
    _log.info(f"Sending SEA{glider} M{mission} to ERDDAP")
    subprocess.check_call(['/usr/bin/bash', "/home/pipeline/utility_scripts/send_to_pipeline.sh", str(glider), str(mission)])


def send_all_complete():
    glider_paths = list(pathlib.Path("/data/data_l0_pyglider/complete_mission").glob("SEA*"))
    for glider_path in glider_paths:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            try:
                glider = int(glider_path.parts[-1][3:])
                mission = int(mission_path.parts[-1][1:])
                send(glider, mission)
            except:
                _log.warning(f"Could not process {mission_path}")


if __name__ == '__main__':
    _log.info("Start sending all to ERDDAP")
    send_all_complete()
    _log.info("Complete send all to ERDDAP")

