import pandas as pd
import xarray as xr
from pathlib import Path
import datetime
import subprocess
import logging
import gsw
from utilities import encode_times
_log = logging.getLogger(__name__)
sender = "/home/pipeline/utility_scripts/send.sh"
if not Path(sender).exists():
    sender = "/home/callum/Documents/data-flow/raw-to-nc/utility_scripts/send.sh"

clean_names = {"Press. [dbar]": "pressure",
               "Temp. [℃]": "temperature",
               "Temp. [deg C]": "temperature",
               "Salinity": "salinity",
               "Sal. [ ]": "salinity",
               "Cond. [mS/cm]": "conductivity",
               'Density [kg/m3]': "density",
               'Density [kg/m^3]': "density",
               'DO [μmol/L]': 'oxygen_concentration',
               'Chl-a [μg/L]': 'chlorophyll',
               'Chl-a [ug/l]': 'chlorophyll',
               'sonde_name': 'sonde_name',
               'sonde_number': 'sonde_number',
               'calibration_date': 'calibration_date',
               'filename': 'filename',
               'latitude': 'latitude',
               'cast_number': 'cast_number',
               'longitude': 'longitude',
               }

attrs_dict = {"sonde_name": {"comment": "model name of CTD"},
              "sonde_number": {"comment": "serial number of CTD"},
              "cast_number": {"comment": "cast number"},
              "calibration_date": {"comment": "date of last calibration"},
              "filename":
                  {"comment": "source filename"},
              "latitude": {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                           'long_name': 'latitude',
                           'observation_type': 'measured',
                           'platform': 'platform',
                           'reference': 'WGS84',
                           'standard_name': 'latitude',
                           'units': 'degrees_north',
                           'valid_max': '90.0',
                           'valid_min': '-90.0', },
              "longitude":
                  {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                   'long_name': 'longitude',
                   'observation_type': 'measured',
                   'platform': 'platform',
                   'reference': 'WGS84',
                   'standard_name': 'longitude',
                   'units': 'degrees_east',
                   'valid_max': '180.0',
                   'valid_min': '-180.0', },

              "oxygen_concentration":
                  {
                      'long_name': 'oxygen concentration',
                      'observation_type': 'calculated',
                      'standard_name': 'mole_concentration_of_dissolved_molecular_oxygen_in_sea_water',
                      'units': 'mmol m-3',
                      'valid_max': '425.',
                      'valid_min': '0.',
                  },
              "chlorophyll":
                  {
                      'long_name': 'chlorophyll',
                      'observation_type': 'calculated',
                      'standard_name': 'concentration_of_chlorophyll_in_sea_water',
                      'units': 'mg m-3',
                      'valid_max': '50.',
                      'valid_min': '0.',
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

              "conductivity": {
                  'instrument': 'instrument_ctd',
                  'long_name': 'water conductivity',
                  'observation_type': 'measured',
                  'standard_name': 'sea_water_electrical_conductivity',
                  'units': 'mS cm-1',
                  'valid_max': '85.',
                  'valid_min': '0.',
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
              "salinity": {'long_name': 'water salinity',
                           'standard_name': 'sea_water_practical_salinity',
                           'units': '1e-3',
                           'comment': 'raw, uncorrected salinity',
                           'sources': 'conductivity temperature pressure',
                           'observation_type': 'calulated',
                           'instrument': 'instrument_ctd',
                           'valid_max': '40.0',
                           'valid_min': '0.0',
                           },
              "temperature":
                  {
                      'long_name': 'water temperature',
                      'observation_type': 'measured',
                      'standard_name': 'sea_water_temperature',
                      'units': 'Celsius',
                      'valid_max': '42',
                      'valid_min': '-5',
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
                df = pd.read_csv(ctd_csv, skiprows=skips, index_col=False, parse_dates={'datetime': ["Measurement Date/Time"]}, date_format=f"%Y{sep}%m{sep}%d %H:%M:%S")
                break
            if line[:4] == "Date":
                skips = i
                df = pd.read_csv(ctd_csv, skiprows=skips, parse_dates={'datetime': ["Date", "Time"]}, date_format="%Y/%m/%d %H:%M:%S")
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
        df = df.rename(columns={"Temp. [deg C]": "Temp. [℃]", "Sal. [ ]": "Salinity", "Density [kg/m^3]": "Density [kg/m3]", "Chl-a [ug/l]": "Chl-a [μg/L]"})
    df_base = pd.concat((df_base, df))
    return df_base


def filenames_match(locfile):
    locations = pd.read_csv(locfile, sep=";")
    csv_dir = locfile.parent / "CSV"
    for fn in locations.File.values:
        filename = fn + ".csv"
        assert (Path(csv_dir) / filename).exists()


def ds_from_df(df):
    ds = xr.Dataset()
    time_attr = {"name": "time"}
    ds['time'] = ('time', df["datetime"], time_attr)

    for col_name in list(df):
        if col_name in clean_names.keys():
            name = clean_names[col_name]
            ds[name] = ('time', df[col_name], attrs_dict[name])
    ds["cast_no"] = ds.cast_number
    ds.attrs = attrs
    ds = encode_times(ds)
    return ds


def main():
    location_files = list(Path("/mnt/samba/").glob("*/5_Calibration/CTD/*cation*.txt"))
    df = pd.DataFrame()
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
    ds = ds_from_df(df)
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
    try:
        main()
    except:
        subprocess.check_call(['/usr/bin/bash', sender, "failed to process ctd data", "process-ctd",
                               "callum.rollo@voiceoftheocean.org"])

    _log.info(f"Complete process ctds")
