import os
import sys
import pathlib
import logging

script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)
from process_pyglider import proc_pyglider_l0

_log = logging.getLogger(__name__)
logging.basicConfig(filename='/data/log/pyglider_all_nrt.log',
                    filemode='a',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


def proc_all_nrt():
    _log.info("Start nrt reprocessing")
    glider_paths = list(pathlib.Path("/data/data_l0_pyglider/nrt").glob("SEA*"))
    glidermissions = []
    for glider_path in glider_paths:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            try:
                glidermissions.append((int(glider_path.parts[-1][3:]), int(mission_path.parts[-1][1:])))
            except:
                _log.warning(f"Could not process {mission_path}")

    for glider, mission in glidermissions:
        input_dir = f"/data/data_raw/nrt/SEA{str(glider).zfill(3)}/{str(mission).zfill(6)}/C-Csv/"
        if not pathlib.Path(input_dir).exists():
            _log.info(f"SEA{glider} M{mission} does not have nrt alseamar raw files. skipping")
            continue
        output_dir = f"/data/data_l0_pyglider/nrt/SEA{glider}/M{mission}/"
        _log.info(f"Reprocessing SEA{glider} M{mission}")
        proc_pyglider_l0(glider, mission, 'sub', input_dir, output_dir)
    _log.info("Finished nrt processing")


if __name__ == '__main__':
    proc_all_nrt()
