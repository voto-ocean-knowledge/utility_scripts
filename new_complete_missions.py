import datetime
import pathlib
import pandas as pd
import numpy as np
import logging
import types
from pyglider_single_mission import process
_log = logging.getLogger(__name__)


def main():
    logf = f'/data/log/new_complete_mission.log'
    logging.basicConfig(filename=logf,
                        filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    _log.info("Check for new missions")
    df_reprocess = pd.read_csv('/home/pipeline/reprocess.csv', parse_dates=["proc_time"])
    df_reprocess.sort_values("proc_time", inplace=True)
    _log.info(f"start length {len(df_reprocess)}")
    glider_paths = list(pathlib.Path("/data/data_raw/complete_mission").glob("SEA*"))
    if len(glider_paths) == len(df_reprocess):
        _log.info("No new missions to process")
        return
    for glider_path in glider_paths:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            glider = int(glider_path.parts[-1][3:])
            mission = int(mission_path.parts[-1][1:])
            a = [np.logical_and(df_reprocess.glider == glider, df_reprocess.mission == mission)]
            if not sum(sum(a)):
                _log.warning(f"new mission {mission_path}")
                args = types.SimpleNamespace()
                args.glider = glider
                args.mission = mission
                args.type = "raw"
                process(args)
                nc_file = list((pathlib.Path(f"/data/data_l0_pyglider/complete_mission/SEA{glider}/M{mission}/timeseries")).glob('*.nc'))[0]
                nc_time = nc_file.lstat().st_mtime
                nc_time = datetime.datetime.fromtimestamp(nc_time)
                new_row = pd.DataFrame({"glider": glider, "mission": mission,
                                        "proc_time": nc_time, "duration": datetime.timedelta(minutes=20)},
                                       index=[len(df_reprocess)])
                df_reprocess = pd.concat((df_reprocess, new_row))
    _log.info(f"end length {len(df_reprocess)}")
    df_reprocess["gm"] = df_reprocess.glider * 10000 + df_reprocess.mission
    df_reprocess = df_reprocess.groupby("gm").first()
    df_reprocess.sort_values("proc_time", inplace=True)
    _log.info("complete")


if __name__ == '__main__':
    main()
