from pathlib import Path
import logging
from pyglider_nrt_from_complete import nrt_proc_from_complete_nc
_log = logging.getLogger(__name__)
logging.basicConfig(filename='/data/log/nrt_from_complete.log',
                    filemode='a',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


def all_nrt_from_complete(reprocess=True):
    _log.info("Start nrt from complete")
    glider_paths = list(Path("/data/data_l0_pyglider/complete_mission").glob("SEA*"))
    glidermissions = []
    for glider_path in glider_paths:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            glider = int(glider_path.parts[-1][3:])
            mission = int(mission_path.parts[-1][1:])
            nrt_path = Path(f"/data/data_raw/nrt/SEA0{str(glider).zfill(2)}/{str(mission).zfill(6)}/C-Csv/")
            out_path = Path(f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/rawnc")
            if nrt_path.exists() and out_path.exists():
                _log.debug(f"nrt path {nrt_path} exists. Skipping")
                continue
            try:
                glidermissions.append((int(glider_path.parts[-1][3:]), int(mission_path.parts[-1][1:])))
            except:
                _log.warning(f"Could not process {mission_path}")
    _log.info(f"will process {len(glidermissions)} missions")
    for glider,  mission in glidermissions:
        out_path = Path(f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/timeseries")
        if out_path.exists() and not reprocess:
            _log.info(f"SEA{glider} M{mission} already exists. Skipping")
            continue
        try:
            nrt_proc_from_complete_nc(glider, mission)
            _log.info(f"Processed SEA{glider} M{mission}")
        except:
            _log.warning(f"failed with SEA{glider} M{mission}")

    _log.info("Complete nrt from complete")


if __name__ == '__main__':
    all_nrt_from_complete()

