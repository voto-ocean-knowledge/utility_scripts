import numpy as np
import datetime
from erddapy import ERDDAP
import pandas as pd
import pathlib
import os
script_dir = pathlib.Path(__file__).parent.absolute()
os.chdir(script_dir)


def main():
    e = ERDDAP(
        server="https://erddap.observations.voiceoftheocean.org/erddap",
        protocol="tabledap",
    )
    # Fetch dataset list
    e.response = "csv"
    e.dataset_id = "allDatasets"
    df_datasets = e.to_pandas(parse_dates=['minTime (UTC)', 'maxTime (UTC)'])
    # drop the allDatasets row and make the datasetID the index for easier reading
    df_datasets.set_index("datasetID", inplace=True)
    df_datasets.drop("allDatasets", inplace=True)
    enough_datasets(df_datasets)
    nrt_vs_complete(df_datasets)
    bad_depths(df_datasets)
    bad_dataset_id(df_datasets)
    delayed = df_datasets.index[df_datasets.index.str[:3] == "del"]
    nrt = df_datasets.index[df_datasets.index.str[:3] == "nrt"]
    num_ds = len(delayed)
    num_nrt = len(nrt)
    unit_check(e, nrt[np.random.randint(0, num_nrt-1)])
    profile_num_vs_dive_num(e, delayed[np.random.randint(0, num_ds-1)])
    sensible_values(e, delayed[np.random.randint(0, num_ds-1)])
    sensible_values(e, delayed[np.random.randint(0, num_ds-1)])
    good_times()
    manual_qc()


def enough_datasets(df_datasets):
    total = int(len(df_datasets))
    nrt = int(sum(df_datasets.index.str[:3] == 'nrt'))
    complete = int(sum(df_datasets.index.str[:3] == 'del'))

    try:
        total_datasets = pd.read_csv("total_datasets.csv")
    except:
        total_datasets = pd.DataFrame({"date": [], "total": [], "nrt": [], "delayed": []})
    new_row = pd.DataFrame({"date": datetime.datetime.now(), "total": total,
                            "nrt": nrt, "delayed": complete},
                           index=[len(total_datasets)])
    if total < total_datasets.total.max():
        print("total has decreaseed")
    total_datasets = pd.concat((total_datasets, new_row))
    total_datasets.to_csv("total_datasets.csv", index=False)


def nrt_vs_complete(df_datasets):
    mtype, glider, mission = [], [], []
    for gm in df_datasets.index:
        mission_type, g, m = gm.split("_")
        mtype.append(mission_type)
        glider.append(int(g[3:]))
        mission.append(int(m[1:]))
    df_datasets["glider"] = glider
    df_datasets["mission_type"] = mtype
    df_datasets["mission"] = mission

    df_nrt = df_datasets[df_datasets.mission_type == "nrt"]
    df_nrt.index = "SEA" + df_nrt.glider.astype(str) + "_M" + df_nrt.mission.astype(str)
    df_delayed = df_datasets[df_datasets.mission_type == "delayed"]
    df_delayed.index = "SEA" + df_delayed.glider.astype(str) + "_M" + df_delayed.mission.astype(str)

    expected_fails = ["SEA61_M63", "SEA67_M15", "SEA66_M45"]
    for this_dataset in df_nrt.index:
        if this_dataset in expected_fails:
            continue
        if this_dataset not in df_delayed.index:
            # print(f"{this_dataset} not found in delayed")
            time_since = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(minutes=0))) - \
                         df_nrt.loc[this_dataset]["maxTime (UTC)"]
            # print("last update: ", time_since)
            if time_since > datetime.timedelta(days=3):
                print(f"unprocessed complete dataset: {this_dataset}. {time_since} since last nrt data")

    for this_dataset in df_delayed.index:
        if this_dataset not in df_nrt.index:
            print(f"{this_dataset} not found in nrt")
            print("last update:", df_delayed.loc[this_dataset]["maxTime (UTC)"])


def bad_depths(df_datasets):
    if len(df_datasets[df_datasets['maxAltitude (m)'] < -2000]['maxAltitude (m)']) > 0:
        print("bad altitude")


def bad_dataset_id(df_datasets):
    # Find missions where the dataset ID number doesn't match the name. This indicates the wrong dataset has been loaded
    for dataset_id, row in df_datasets.iterrows():
        name = row["title"]
        if "adcp" in dataset_id:
            print("skip namecheck for adcp data")
            continue
        if " " in name:
            num_title = int(name[3:6])
        else:
            num_title = int(name.split("-")[0][-2:])
        num_id = int(dataset_id.split("_")[1][-3:])
        if num_id != num_title:
            print(name, dataset_id)


def profile_num_vs_dive_num(e, dataset_id):
    e.variables = [
        "time",
        "pressure",
        "depth",
        "dive_num",
        "profile_direction",
        "profile_index",
        "profile_num",
    ]
    e.dataset_id = dataset_id

    ds = e.to_xarray()
    ds = ds.drop_dims("timeseries")
    ds_sort = ds.sortby('time')
    if not ds.time.equals(ds_sort.time):
        print("datasets not sorted by time")
    profiles = len(np.unique(ds.profile_num)) 
    dives = len(np.unique(ds.dive_num))
    if not profiles / 2 == dives:
        print(f"missmatch between {dataset_id} profile_num {profiles} and dive_num {dives} ({dives*2})")


def sensible_values(e, dataset_id):
    e.dataset_id = dataset_id

    ds = e.to_xarray()
    ds = ds.drop_dims("timeseries")
    for var in list(ds.variables):
        mini, maxi = np.nanmin(ds[var].values), np.nanmax(ds[var].values)
        low_lim = -1e3
        high_lim = 1e4
        if type(mini) == np.datetime64:
            low_lim = np.datetime64('2020-01-31T09:02:47.457999872')
            high_lim = np.datetime64('2030-01-31T09:02:47.457999872')
        if var == "internal_pressure":
            high_lim = 1e5
        if var == "desired_heading":
            low_lim = -1e5
        if mini < low_lim:
            print(f"{dataset_id} bad var {var} minimum {mini}")
        if maxi > high_lim:
            print(f"{dataset_id} bad var {var} maximum {maxi}")


def unit_check(e, dataset_id):
    e.dataset_id = dataset_id

    ds = e.to_xarray()
    ds = ds.drop_dims("timeseries")
    for var in list(ds.variables):
        attrs = ds[var].attrs
        if var == "oxygen_concentration":
            if attrs["units"] != "mmol m-3":
                print(f"bad oxy units {attrs['units']}")


def good_times():
    print("TODO: check times of all datetime like columns are in expected range")


def manual_qc():
    print("TODO: check that manually applied QC to vars like oxygen has been applied")


if __name__ == '__main__':
    main()
