import pandas as pd
import xarray as xr
from pathlib import Path
import datetime
import subprocess
import logging
import gsw
from utilities import encode_times
from seabird.cnv import fCNV
import pathlib
import sys
script_dir = pathlib.Path(__file__).parent.absolute()
parent_dir = script_dir.parents[0]
qc_dir = parent_dir / "voto_glider_qc"
sys.path.append(str(qc_dir))
# noinspection PyUnresolvedReferences
from flag_qartod import flag_ioos, ioos_qc
_log = logging.getLogger(__name__)
sender = "/home/pipeline/utility_scripts/send.sh"
if not Path(sender).exists():
    sender = "/home/callum/Documents/data-flow/raw-to-nc/utility_scripts/send.sh"

clean_names = {
    "latitude": "LATITUDE",
    "Latitude": "LATITUDE",
    'longitude': 'LONGITUDE',
    'Longitude': 'LONGITUDE',
    'oxygen_concentration': 'DOXY',
    'DO [μmol/L]': 'DOXY',
    'chlorophyll_concentration': 'CHLA',
    'time': 'TIME',
    'depth': 'DEPTH',
    "Press. [dbar]": "pressure",
    "prDM": "pressure",
    "prdM": "pressure",
    "Temp. [℃]": "TEMP",
    "Temp. [deg C]": "TEMP",
    "t090C": "TEMP",
    "tv290C": "TEMP",
    "Salinity": "PSAL",
    "Sal. [ ]": "PSAL",
    "Cond. [mS/cm]": "CNDC",
    "c0S/m": "CNDC",
    'Density [kg/m3]': "density",
    'Density [kg/m^3]': "density",
    'Chl-a [μg/L]': 'CHLA',
    'Chl-a [ug/l]': 'CHLA',
    'sonde_name': 'sonde_name',
    'sonde_number': 'sonde_number',
    'calibration_date': 'calibration_date',
    'filename': 'filename',
    'cast_number': 'cast_number',
    "datetime_first_scan": "TIME",
    "datetime": "TIME",
}

attrs_dict = {"sonde_name": {"comment": "model name of CTD"},
              "sonde_number": {"comment": "serial number of CTD"},
              "cast_number": {"comment": "cast number"},
              "calibration_date": {"comment": "date of last calibration"},
              "filename":
                  {"comment": "source filename"},
              "LATITUDE": {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                           'long_name': 'latitude',
                           'observation_type': 'measured',
                           'platform': 'platform',
                           'reference': 'WGS84',
                           'standard_name': 'latitude',
                           'units': 'degrees_north',
                           'valid_max': 90,
                           'valid_min': 90,
                           'axis': 'Y',
                           },
              "LONGITUDE":
                  {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                   'long_name': 'longitude',
                   'observation_type': 'measured',
                   'platform': 'platform',
                   'reference': 'WGS84',
                   'standard_name': 'longitude',
                   'units': 'degrees_east',
                   'valid_max': 180,
                   'valid_min': -180,
                   'axis': 'X',
                   },

              "DOXY":
                  {
                      'long_name': 'oxygen concentration',
                      'observation_type': 'calculated',
                      'standard_name': 'mole_concentration_of_dissolved_molecular_oxygen_in_sea_water',
                      'units': 'mmol m-3',
                      'valid_max': 425,
                      'valid_min': 0,
                      'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/DOXY/',
                  },
              "CHLA":
                  {
                      'long_name': 'chlorophyll',
                      'observation_type': 'calculated',
                      'standard_name': 'concentration_of_chlorophyll_in_sea_water',
                      'units': 'mg m-3',
                      'valid_max': 50,
                      'valid_min': 0,
                      'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/CPWC/',
                  },
              "density": {'long_name': 'Density',
                          'standard_name': 'sea_water_density',
                          'units': 'kg m-3',
                          'comment': 'raw, uncorrected salinity',
                          'observation_type': 'calculated',
                          'sources': 'salinity temperature pressure',
                          'valid_min': '1000.0',
                          'valid_max': '1040.0',
                          },

              "CNDC": {
                  'instrument': 'instrument_ctd',
                  'long_name': 'water conductivity',
                  'observation_type': 'measured',
                  'standard_name': 'sea_water_electrical_conductivity',
                  'units': 'mS cm-1',
                  'valid_max': '85.',
                  'valid_min': '0.',
                  'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/CNDC/',
              },
              "pressure": {
                  'comment': 'ctd pressure sensor',
                  'instrument': 'instrument_ctd',
                  'long_name': 'water pressure',
                  'observation_type': 'measured',
                  'positive': 'down',
                  'reference_datum': 'sea-surface',
                  'standard_name': 'sea_water_pressure',
                  'units': 'dbar',
                  'valid_max': '1000',
                  'valid_min': '0',
              },
              "PSAL": {'long_name': 'water salinity',
                       'standard_name': 'sea_water_practical_salinity',
                       'units': '1e-3',
                       'comment': 'raw, uncorrected salinity',
                       'sources': 'conductivity temperature pressure',
                       'observation_type': 'calculated',
                       'instrument': 'instrument_ctd',
                       'valid_max': '40.0',
                       'valid_min': '0.0',
                       'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/PSAL/',
                       },
              "TEMP":
                  {
                      'long_name': 'water temperature',
                      'observation_type': 'measured',
                      'standard_name': 'sea_water_temperature',
                      'units': 'Celsius',
                      'valid_max': '42',
                      'valid_min': '-5',
                      'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/TEMP/',
                  },

              }

date_created = str(datetime.datetime.now())
attrs = {
    'acknowledgement': 'This study used data collected and made freely available by Voice of the Ocean Foundation ('
                       'https://voiceoftheocean.org) accessed from '
                       'https://erddap.observations.voiceoftheocean.org/erddap/index.html',
    'creator_email': 'callum.rollo@voiceoftheocean.org',
    'source': 'Observational data from handheld CTD casts',
    'creator_name': 'Callum Rollo',
    'creator_url': 'https://observations.voiceoftheocean.org',
    'date_created': date_created,
    'date_issued': date_created,
    'institution': 'Voice of the Ocean Foundation',
    'keywords': 'CTD, Oceans, Ocean Pressure, Water Pressure, Ocean Temperature, Water Temperature, Salinity/Density, '
                'Conductivity, Density, Salinity',
    'keywords_vocabulary': 'GCMD Science Keywords',
    "title": "CTD from glider deployment/recovery",
    'disclaimer': "Data, products and services from VOTO are provided 'as is' without any warranty as to fitness for "
                  "a particular purpose."}


def read_ctd(ctd_csv, locfile, df_base):
    if df_base.isnull().all().all():
        cast_number = 0
    else:
        cast_number = df_base.cast_number.max() + 1
    locations = pd.read_csv(locfile, sep=";")
    fn = ctd_csv.name.split(".")[0]
    row = locations[locations.File == fn].iloc[0]
    sep = "/"
    with open(ctd_csv) as file:
        for i, line in enumerate(file):
            if "File Date" in line:
                if "-" in line:
                    sep = "-"
            if "Measurement" in line:
                skips = i
                df = pd.read_csv(ctd_csv, skiprows=skips, index_col=False,
                                 parse_dates={'datetime': ["Measurement Date/Time"]},
                                 date_format=f"%Y{sep}%m{sep}%d %H:%M:%S")
                break
            if line[:4] == "Date":
                skips = i
                df = pd.read_csv(ctd_csv, skiprows=skips, parse_dates={'datetime': ["Date", "Time"]},
                                 date_format="%Y/%m/%d %H:%M:%S")
                break
    with open(ctd_csv) as file:
        for i, line in enumerate(file):
            if "SondeName=ASTD152-ALC-R02" in line:
                df["sonde_name"] = line.split("=")[1][:-1]
            if "SondeNo" in line:
                df["sonde_number"] = line.split("=")[1][:-1]
            if "CoefDate=2022/12/15" in line:
                df["calibration_date"] = line.split("=")[1][:-1]
                break
    df['calibration_date'] = pd.to_datetime(df['calibration_date'], format='%Y/%m/%d')
    df["filename"] = row.File
    df["longitude"] = row.Longitude
    df["latitude"] = row.Latitude
    df["cast_number"] = cast_number
    if 'Depth [m]' in list(df):
        df["Press. [dbar]"] = gsw.p_from_z(-df['Depth [m]'], row.Latitude)
    if not 'DO [μmol/L]' in list(df):
        df['DO [μmol/L]'] = df['Weiss-DO [mg/l]'] * 31.252
        df = df.rename(
            columns={"Temp. [deg C]": "Temp. [℃]", "Sal. [ ]": "Salinity", "Density [kg/m^3]": "Density [kg/m3]",
                     "Chl-a [ug/l]": "Chl-a [μg/L]"})
    for col in list(df):
        if col in clean_names.keys():
            df = df.rename(columns={col: clean_names[col]})
    df = df[list(set(list(attrs_dict.keys()) + ["TIME"]).intersection(set(list(df))))]
    df["TIME"] = df.TIME.astype('datetime64[ns]')
    df_base = pd.concat((df_base, df))
    return df_base


def load_cnv_file(ctd_csv, df_base):
    profile = fCNV(ctd_csv)

    df = profile.as_DataFrame()

    if df_base.isnull().all().all():
        cast_number = 0
    else:
        cast_number = df_base.cast_number.max() + 1
    fn = ctd_csv.name.split(".")[0]
    attrs = profile.attrs

    if "sbe_model" in attrs.keys():
        df["sonde_name"] = "SBE" + attrs["sbe_model"]
    else:
        df["sonde_name"] = ctd_csv.name.split("_")[0]
    df["filename"] = fn
    df["cast_number"] = cast_number
    if not 'DO [μmol/L]' in list(df):
        df['DO [μmol/L]'] = df['oxygen_ml_L'] * 22.39244
    for col in list(df):
        if col in clean_names.keys():
            df = df.rename(columns={col: clean_names[col]})

    df = df[list(set(list(attrs_dict.keys()) + ["TIME"]).intersection(set(list(df))))]
    df["TIME"] = df.TIME.astype('datetime64[ns]')
    df_base = pd.concat((df_base, df))
    return df_base


def filenames_match(locfile):
    locations = pd.read_csv(locfile, sep=";")
    csv_dir = locfile.parent / "CSV"
    for fn in locations.File.values:
        filename = fn + ".csv"
        assert (Path(csv_dir) / filename).exists()


def ds_from_df(df):
    ds = xr.Dataset(coords={'time': ('time', df["TIME"])})
    time_attr = {"name": "time"}
    ds['time'] = ('time', df["TIME"], time_attr)

    for col_name in list(df):
        if col_name in attrs_dict.keys():
            ds[col_name] = ('time', df[col_name], attrs_dict[col_name])
    ds["cast_no"] = ds.cast_number
    ds.attrs = attrs
    ds = encode_times(ds)
    return ds

def flag_ctd(ds):
    ds = flag_ioos(ds)
    ds.attrs["processing_level"] = f"L1. Quality control flags from IOOS QC QARTOD https://github.com/ioos/ioos_qc " \
                                   f"Version: {ioos_qc.__version__} "
    ds.attrs["disclaimer"] = "Data, products and services from VOTO are provided 'as is' without any warranty as" \
                             " to fitness for a particular purpose."
    return ds
def main():
    location_files = list(Path("/mnt/samba/").glob("*/5_Calibration/CTD/*cation*.txt"))
    df = pd.DataFrame()
    fn = 0
    cnv_files = list(Path("/mnt/samba/").glob("*/5_Calibration/*/*SBE09*.cnv*")) + list(
        Path("/mnt/samba/").glob("*/5_Calibration/*/*SBE19*EDITED*.cnv*"))
    for filename in cnv_files:
        _log.info(f"Start add cnv {filename}")
        try:
            df = load_cnv_file(filename, df)
        except:
            _log.error(f"failed with {filename}")
            subprocess.check_call(
                ['/usr/bin/bash', sender, f"failed to process ctd {filename}", "ctd-process",
                 "callum.rollo@voiceoftheocean.org"])
            continue
        _log.info(f"Added {filename}")
        fn += 1
    for locfile in location_files:
        _log.info(f"processing location file {locfile}")
        csv_dir = locfile.parent / "CSV"
        csv_files = list(csv_dir.glob("*.*sv"))
        filenames_match(locfile)
        for ctd_csv in csv_files:
            _log.info(f"Start add {ctd_csv}")
            try:
                df = read_ctd(ctd_csv, locfile, df)
            except:
                _log.error(f"failed with {ctd_csv}")
                subprocess.check_call(
                    ['/usr/bin/bash', sender, f"failed to process ctd {ctd_csv}", "ctd-process",
                     "callum.rollo@voiceoftheocean.org"])
                continue
            _log.info(f"Added {ctd_csv}")
            fn += 1
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
    ds.to_netcdf("/mnt/samba/processed/ctd_deployment.nc")
    _log.info(f"Send ctds to ERDDAP")
    subprocess.check_call(
        ['/usr/bin/rsync', "/mnt/samba/processed/ctd_deployment.nc",
         "usrerddap@13.51.101.57:/media/data/ctd/ctd_deployment.nc"])


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
