import subprocess
import logging
import pandas as pd
from pathlib import Path
_log = logging.getLogger(__name__)
from votoutils.ctd.ctd import load_cnv_file, filenames_match, read_ctd, ds_from_df, flag_ctd
from votoutils.utilities.utilities import encode_times, mailer


def main():
    location_files = list(Path("/mnt/samba/").glob("*/5_Calibration/CTD/*cation*.txt"))
    missing_ctd_files = []
    casts = []
    fn = 0
    cnv_files = list(Path("/mnt/samba/").glob("*/5_Calibration/*/*SBE09*.cnv*")) + list(
        Path("/mnt/samba/").glob("*/5_Calibration/*/*SBE19*EDITED*.cnv*"))
    for filename in cnv_files:
        _log.info(f"Start add cnv {filename}")
        try:
            casts.append(load_cnv_file(filename))
        except:
            _log.error(f"failed with {filename}")
            mailer("ctd-process", f"failed to process ctd {filename}")
            continue
        _log.info(f"Added {filename}")
        fn += 1
    df_all_locs = pd.DataFrame()
    for locfile in location_files:
        df_loc = pd.read_csv(locfile, sep=';')
        df_loc['fn'] = locfile
        df_all_locs = pd.concat((df_all_locs, df_loc))
        _log.info(f"processing location file {locfile}")
        csv_dir = locfile.parent / "CSV"
        csv_files = list(csv_dir.glob("*.*sv"))
        missing_ctd_files = filenames_match(locfile, missing_files=missing_ctd_files)
        for ctd_csv in csv_files:
            _log.info(f"Start add {ctd_csv}")
            try:
                casts.append(read_ctd(ctd_csv, locfile))
            except:
                _log.error(f"failed with {ctd_csv}")
                mailer("ctd-process", f"failed to process ctd {csv_files}")
                continue
            _log.info(f"Added {ctd_csv}")
            fn += 1
    # renumber profiles, so that profile_num still is unique in concat-dataset
    for index, cast in enumerate(casts):
        cast['cast_number'] = index
    df = pd.concat(casts)
    ds = ds_from_df(df)
    _log.info(f"total ctds = {fn}")
    rename_dict = {#'TIME': "time",
                   'TEMP': "temperature",
                   'DOXY': "oxygen_concentration",
                   'CNDC': "conductivity",
                   'LONGITUDE': "longitude",
                   'LATITUDE': "latitude",
                   'PSAL': "salinity",
                   'CHLA': "chlorophyll",
                   }
    ds = ds.rename(rename_dict)
    ds = flag_ctd(ds)
    ds = encode_times(ds)
    ds.to_netcdf("/data/ctd/ctd_deployment.nc")
    _log.info(f"Send ctds to ERDDAP")
    subprocess.check_call(
        ['/usr/bin/rsync', "/data/ctd/ctd_deployment.nc",
         "usrerddap@136.243.54.252:/data/ctd/ctd_deployment.nc"])
    if not len(df_all_locs) != len(df_all_locs['File'].unique()):
        mailer("bad-ctd-locfiles", f"duplicate entries accross ctd location files")

    if len(missing_ctd_files) > 0:
        mailer("missing-ctd", f"missing ctd file {', '.join(missing_ctd_files)}. present in location file")


if __name__ == '__main__':
    logf = f'/data/log/process_ctd.log'
    logging.basicConfig(filename=logf,
                        filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    _log.info(f"Start process ctds")
    main()
    _log.info(f"Complete process ctds")
