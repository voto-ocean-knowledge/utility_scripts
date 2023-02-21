from pathlib import Path
import pandas as pd
import numpy as np
import utilities


def compute_glider_stats():
    try:
        df_stats = pd.read_csv('/home/pipeline/stats.csv')
    except:
        df_stats = pd.DataFrame({"glider": [], "mission": [], "legato_freq": []})

    glider_paths = list(Path("/data/data_raw/complete_mission").glob("SEA*"))
    for glider_path in glider_paths:
        mission_paths = glider_path.glob("M*")
        for mission_path in mission_paths:
            glider = int(glider_path.parts[-1][3:])
            mission = int(mission_path.parts[-1][1:])
            a = [np.logical_and(df_stats.glider == glider, df_stats.mission == mission)]
            if sum(sum(a)):
                continue
            try:
                legato_sampling_freq = utilities.ctd_sampling_period(glider, mission)
                new_row = pd.DataFrame({"glider": glider, "mission": mission,
                                        "legato_freq": legato_sampling_freq},
                                       index=[len(df_stats)])
                df_stats = pd.concat((df_stats, new_row))
            except:
                print(f"fail for SEA{glider} M{mission}")
    df_stats.to_csv('/home/pipeline/stats.csv', index=False)


if __name__ == '__main__':
    compute_glider_stats()
