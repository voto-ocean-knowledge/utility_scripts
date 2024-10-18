import pandas as pd
import xarray as xr
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
import pynmea2
import datetime


def parse_nrt():
    df = pd.read_csv("SB2120/AUTO/DATA.TXT", skiprows=1)
    df = pd.read_csv("SB2120/AUTO/DATA.TXT", skiprows=1, names=np.arange(df.shape[1]))
    og_cols = df.columns.copy()
    for col_name in og_cols[:-1]:
        col = df[col_name]
        item = col[0]
        new_name = item.split(' = ')[0]
        df[new_name] = col.str[len(new_name) + 3:]
    df = df.drop(og_cols, axis=1)
    df = df.replace('NULL ', 'NaN')
    df = df.dropna()
    df['Time'] = pd.to_datetime(df.Time, dayfirst=True)
    for col_name in df.columns:
        if col_name in ['Time']:
            continue
        df[col_name] = df[col_name].astype(float)
    for col_name in df.columns:
        if col_name in ['Time']:
            continue
        try:
            if len(df[col_name].unique()) == len(df[col_name].astype(int).unique()):
                df[col_name] = df[col_name].astype(int)
        except:
            continue
    auto = df.set_index('Time')

    df = pd.read_csv("SB2120/DATA/DATA.TXT", skiprows=1)
    df = pd.read_csv("SB2120/DATA/DATA.TXT", skiprows=1, names=np.arange(df.shape[1]))
    og_cols = df.columns.copy()
    for col_name in og_cols[:-1]:
        col = df[col_name]
        item = col[0]
        new_name = item.split(' = ')[0]
        df[new_name] = col.str[len(new_name) + 3:]
    df = df.drop(og_cols, axis=1)
    df = df.replace('NULL ', 'NaN')
    df = df.dropna()
    df['Time'] = pd.to_datetime(df.Time, dayfirst=True)
    for col_name in df.columns:
        if col_name in ['Time']:
            continue
        df[col_name] = df[col_name].astype(float)
    for col_name in df.columns:
        if col_name in ['Time']:
            continue
        try:
            if len(df[col_name].unique()) == len(df[col_name].astype(int).unique()):
                df[col_name] = df[col_name].astype(int)
        except:
            continue
    data = df.set_index('Time')
    mose_nrt = data[['Hs', 'Ts', 'T0', 'Hmax', 'Err']].rename({'Hs': 'significant_wave_height',
                                                               'Ts': 'significant_wave_period',
                                                               'T0': 'mean_wave_period',
                                                               'Hmax': 'maximum_wave_height',
                                                               'Err': 'percentage_error_lines'}, axis=1).dropna(
        subset=['significant_wave_height'])
    mose_nrt.to_parquet("intermediate_data/mose_nrt.parquet")
    df_nrt = data.join(auto, how='outer', lsuffix='_data', rsuffix='_auto')
    df_nrt.to_csv('data_out/nrt.csv')


def parse_legato():
    df_legato = pd.read_csv('SB2120/LEGATO/207496_20240830_1456_data.txt', parse_dates=['Time']).set_index(
        'Time').rename({'Pressure': 'pressure_legato'}, axis=1)
    df_legato.to_parquet('intermediate_data/legato.pqt')


def parse_gmx560():
    dt = datetime.datetime(1970, 1, 1)
    messages = {"GPGGA": [], 'PGILT': [], 'WIHDM': [], 'WIMWVR': [], 'WIMWVT': [], 'WIXDRC': [], 'WIXDRA': []}
    with open("SB2120/DATA/GMX560.TXT", encoding='latin') as infile:
        for line in infile.readlines():
            if 'Sensorlog opened' in line:
                dt = datetime.datetime.strptime(line[-20:-1], '%d.%m.%Y %H:%M:%S')
            try:
                msg = pynmea2.parse(line, check=True)
            except pynmea2.ParseError as e:
                continue
            if not msg:
                continue
            if msg.identifier() == 'GPGGA,':
                timestamp = msg.data[0]
                if len(timestamp) > 6:
                    dt = datetime.datetime(dt.year, dt.month, dt.day, int(timestamp[:2]), int(timestamp[2:4]),
                                           int(timestamp[4:6]))
            talker = line.split(',')[0][1:]
            if talker == 'WIXDR':
                talker += line.split(',')[1]
            if talker == 'WIMWV':
                talker += line.split(',')[2]
            messages[talker].append(f"{dt},{line}")

    for talker, lines in messages.items():
        with open(f"intermediate_data/GMX560/{talker}.txt", mode='w') as outfile:
            outfile.writelines(lines)


def merge_gmx560():
    df_wind_rel = pd.read_csv('intermediate_data/GMX560/WIMWVR.txt',
                              names=['datetime', 'talker', 'wind_direction_relative', 'relative', 'windspeed_relative',
                                     'unit', 'acceptable_measurement'], parse_dates=['datetime']).set_index('datetime')
    df_wind_rel = df_wind_rel[df_wind_rel['acceptable_measurement'].str[0] == 'A'][
        ['wind_direction_relative', 'windspeed_relative']]
    df_wind_true = pd.read_csv('intermediate_data/GMX560/WIMWVT.txt',
                               names=['datetime', 'talker', 'wind_direction_true', 'relative', 'windspeed_true', 'unit',
                                      'acceptable_measurement'], parse_dates=['datetime']).set_index('datetime')
    df_wind_true = df_wind_true[df_wind_true['acceptable_measurement'].str[0] == 'A'][
        ['wind_direction_true', 'windspeed_true', ]]
    df_heading = pd.read_csv('intermediate_data/GMX560/WIHDM.txt',
                             names=['datetime', 'talker', 'heading_magnetic', ' magnetic', 'checksum'],
                             parse_dates=['datetime']).set_index('datetime')
    df_heading = df_heading[['heading_magnetic', ]]
    df_attitude = pd.read_csv('intermediate_data/GMX560/WIXDRA.txt',
                              names=['datetime', 'talker', 'a', 'pitch', 'b', 'c', 'd', 'roll', 'e', 'f'],
                              parse_dates=['datetime']).set_index('datetime')
    df_attitude = df_attitude[['pitch', 'roll']]
    df_weather = pd.read_csv('intermediate_data/GMX560/WIXDRC.txt',
                             names=['datetime', 'talker', 'a', 'air_temperature', 'b', 'c', 'd', 'air_pressure', 'e',
                                    'f', 'g', 'humidity_%', 'i', 'j'], parse_dates=['datetime']).set_index('datetime')
    df_weather = df_weather[['air_temperature', 'air_pressure', 'humidity_%']]
    df_weather_gps = pd.read_csv('intermediate_data/GMX560/GPGGA.txt',
                                 names=['datetime', 'talker', 'timestamp', 'lat_str', 'b', 'lon_str', 'd', 'n', 'e',
                                        'f', 'g', 'm', 'i', 'j', 'k', 'l'], parse_dates=['datetime']).set_index(
        'datetime')
    df_weather_gps = df_weather_gps[['timestamp', 'lat_str', 'lon_str']]
    df_tilt = pd.read_csv('intermediate_data/GMX560/PGILT.txt',
                          names=['datetime', 'talker', 'a', 'eastward_tilt', 'b', 'northward_tilt', 'd',
                                 'vertical_orientation', 'e'], parse_dates=['datetime']).set_index('datetime')
    df_tilt = df_tilt[['eastward_tilt', 'northward_tilt', 'vertical_orientation']]

    df_gmx = df_wind_rel.sort_index()
    for df_add in [df_wind_true, df_heading, df_attitude, df_weather, df_weather_gps, df_tilt]:
        df_add = df_add.sort_index()
        df_gmx = pd.merge_asof(df_gmx, df_add, left_index=True, right_index=True, direction='nearest',
                               tolerance=pd.Timedelta("1s"))
    df_gmx.to_parquet('intermediate_data/gmx.pqt')


def parse_mose():
    with open("SB2120/DATA/MOSE.TXT", encoding='latin') as infile:
        with open("intermediate_data/mose_good.txt", 'w') as outfile:
            with open("intermediate_data/mose_loc.txt", 'w') as locfile:
                for line in infile.readlines():
                    try:
                        msg = pynmea2.parse(line, check=True)
                        if msg.data[1] == 'MOT' and msg.data[3] != '80':
                            goodline = line.replace(' ', '')[:-4] + '\n'
                            outfile.write(goodline)
                        if msg.data[1] == 'POS':
                            goodline = line.replace(' ', '')[:-4] + '\n'
                            locfile.write(goodline)
                    except pynmea2.ParseError as e:
                        # print('Parse error: {}'.format(e))
                        continue


def merge_mose():
    mose = pd.read_csv('intermediate_data/mose_good.txt',
                       names=['manufactuer', 'sentence_type', 'frequency', 'year', 'month', 'day', 'hour', 'minute',
                              'second', 'vert_m', 'north_m', 'west_m', 'flag'], encoding='latin')
    mose['year'] = 2000 + mose['year']
    mose['datetime'] = pd.to_datetime(mose[['year', 'month', 'day', 'hour', 'minute', 'second']])
    mose = mose[mose['flag'] == 0]  # remove bad flagged data (from mose manual)
    mose = mose[['frequency', 'datetime', 'vert_m', 'north_m', 'west_m']].set_index('datetime').sort_index()
    mose_high_freq = mose[mose['frequency'] == 'HF'][['vert_m', 'north_m', 'west_m']]
    mose_low_freq = mose[mose['frequency'] == 'LF'][['vert_m', 'north_m', 'west_m']]
    mose_loc = pd.read_csv('intermediate_data/mose_loc.txt',
                           names=['manufactuer', 'sentence_type', 'year', 'month', 'day', 'hour', 'minute', 'second',
                                  'lat_deg', 'lat_min', 'lat_dir', 'lon_deg', 'lon_min', 'lon_dir', 'height', 'hdop',
                                  'vdop'], encoding='latin')
    mose_loc['year'] = 2000 + mose_loc['year']
    mose_loc['datetime'] = pd.to_datetime(mose_loc[['year', 'month', 'day', 'hour', 'minute', 'second']])
    mose_loc = mose_loc.set_index('datetime').sort_index()
    mose_loc['lon'] = mose_loc['lon_deg'] + mose_loc['lon_min'] / 60
    mose_loc['lat'] = mose_loc['lat_deg'] + mose_loc['lat_min'] / 60
    mose_loc = mose_loc[mose_loc['height'] > -100]
    mose_loc = mose_loc[['lat', 'lon', 'height', 'hdop', 'vdop']]
    df_mose = mose_high_freq.join(mose_loc, how='outer')
    df_mose.to_parquet('intermediate_data/mose.pqt')


def merge_sensors():
    df_legato = pd.read_parquet('intermediate_data/legato.pqt')
    df_mose = pd.read_parquet('intermediate_data/mose.pqt')
    df_gmx = pd.read_parquet('intermediate_data/gmx.pqt')
    df_mose_nrt = pd.read_parquet('intermediate_data/mose_nrt.parquet')
    df_delayed = df_legato.sort_index()
    df_delayed = pd.merge_asof(df_delayed, df_mose, left_index=True, right_index=True, direction='nearest',
                               tolerance=pd.Timedelta("1s"))
    df_delayed = pd.merge_asof(df_delayed, df_gmx, left_index=True, right_index=True, direction='nearest',
                               tolerance=pd.Timedelta("1s"))
    df_delayed = pd.merge_asof(df_delayed, df_mose_nrt, left_index=True, right_index=True, direction='nearest',
                               tolerance=pd.Timedelta("1s"))
    df_delayed = df_delayed[
        ['Conductivity', 'Temperature', 'pressure_legato', 'vert_m', 'north_m', 'west_m', 'lat', 'lon',
         'wind_direction_relative', 'windspeed_relative', 'wind_direction_true', 'windspeed_true', 'heading_magnetic',
         'pitch', 'roll', 'air_temperature', 'air_pressure', 'humidity_%', 'significant_wave_height',
         'significant_wave_period', 'mean_wave_period', 'maximum_wave_height', 'percentage_error_lines']]

    df_delayed.to_parquet('data_out/delayed.pqt')


def get_attrs(ds):
    date_created = datetime.datetime.now().isoformat().split('.')[0]
    attrs = {
        'platform': 'sub-surface gliders',
        'platform_vocabulary': 'https://vocab.nerc.ac.uk/collection/L06/current/27/',
        'platform_serial_number': 'SB2120',
        'acknowledgement': 'This study used data collected and made freely available by Voice of the Ocean Foundation ('
                           'https://voiceoftheocean.org)',
        'area': 'Baltic Sea',
        'cdm_data_type': 'TrajectoryProfile',
        'conventions': 'CF-1.10',
        'institution_country': 'SWE',
        'creator_email': 'callum.rollo@voiceoftheocean.org',
        'creator_name': 'Callum Rollo',
        'creator_type': 'Person',
        'creator_url': 'https://observations.voiceoftheocean.org',
        'date_created': date_created,
        'date_issued': date_created,
        'geospatial_lat_max': np.nanmax(ds.LATITUDE),
        'geospatial_lat_min': np.nanmin(ds.LATITUDE),
        'geospatial_lat_units': 'degrees_north',
        'geospatial_lon_max': np.nanmax(ds.LONGITUDE),
        'geospatial_lon_min': np.nanmin(ds.LONGITUDE),
        'geospatial_lon_units': 'degrees_east',
        'infoUrl': 'https://observations.voiceoftheocean.org',
        'inspire': "ISO 19115",
        'institution': 'Voice of the Ocean Foundation',
        'institution_edmo_code': '5579',
        'keywords': 'CTD, Oceans, Ocean Pressure, Water Pressure, Ocean Temperature, Water Temperature, Salinity/Density, '
                    'Conductivity, Density, Salinity',
        'keywords_vocabulary': 'GCMD Science Keywords',
        'licence': 'Creative Commons Attribution 4.0 (https://creativecommons.org/licenses/by/4.0/) This study used data collected and made freely available by Voice of the Ocean Foundation (https://voiceoftheocean.org) accessed from https://erddap.observations.voiceoftheocean.org/erddap/index.html',
        "title": "Sailbuoy data from the Baltic",
        'disclaimer': "Data, products and services from VOTO are provided 'as is' without any warranty as to fitness for "
                      "a particular purpose.",
        'QC_indicator': 'L1',
        'references': 'Voice of the Ocean Foundation',
        'source': 'Voice of the Ocean Foundation',
        'sourceUrl': 'https://observations.voiceoftheocean.org',
        'standard_name_vocabulary': 'CF Standard Name Table v70',
        'time_coverage_end': str(np.nanmax(ds.time)).split(".")[0],
        'time_coverage_start': str(np.nanmin(ds.time)).split(".")[0],
        'variables': list(ds)
    }
    ts = pd.to_datetime(attrs['time_coverage_start']).strftime('%Y%m%dT%H%M')
    postscript = 'delayed'
    attrs['id'] = f"{attrs['platform_serial_number']}_{ts}_{postscript}"
    return attrs


clean_names = {
    "lat": "LATITUDE",
    'lon': 'LONGITUDE',
    'time': 'TIME',
    'pitch': 'PITCH',
    'roll': 'ROLL',
    'heading_magnetic': 'HEADING',
    'pressure_legato': 'PRES',
    'Conductivity': 'CNDC',
    'oxygen_concentration': 'DOXY',
    'chlorophyll': 'CHLA',
    'Temperature': 'TEMP',
    'wind_direction_true': 'WIND_DIRECTION',
    'windspeed_true': 'WIND_SPEED',
    'air_temperature': 'TEMP_AIR',
    'air_pressure': 'PRESSURE_AIR',
    'humidity_%': 'HUMIDITY',
    'significant_wave_height': 'significant_wave_height',
    'significant_wave_period': 'significant_wave_period',
    'mean_wave_period': 'mean_wave_period',
    'maximum_wave_height': 'maximum_wave_height',
    'percentage_error_lines': 'percentage_error_lines',
    'vert_m': 'vertical_displacement',
    'north_m': 'northward_displacement',
    'west_m': 'westward_displacement',
}
attrs_dict = {
    'vertical_displacement': {'long_name': 'Vertical displacement of platform, from MOSE sensor',
                              "sensor": "sensor_wave",
                              "units": "m"},
    'northward_displacement': {'long_name': 'northward displacement of platform, from MOSE sensor',
                               "sensor": "sensor_wave",
                               "units": "m"},
    'westward_displacement': {'long_name': 'northward displacement of platform, from MOSE sensor',
                              "sensor": "sensor_wave",
                              "units": "m"},
    'mean_wave_period': {'long_name': 'mean wave period, from MOSE sensor',
                         "sensor": "sensor_wave",
                         "units": "s"},
    'maximum_wave_height': {'long_name': 'maximum wave height, from MOSE sensor',
                            "sensor": "sensor_wave",
                            "units": "m"},
    'percentage_error_lines': {'long_name': 'percentage of error lines from MOSE sensor, used to calculate wave stats',
                               "sensor": "sensor_wave",
                               "units": "percent"},
    'significant_wave_period': {
        'long_name': 'Zero-crossing period of waves (highest one third) {significant wave period Ts} on the water body',
        "sensor": "sensor_wave",
        'units': 's',
        'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/GTZHZZ01/'},
    'significant_wave_height': {'long_name': 'Significant wave height of waves {Hs} on the water body',
                                "sensor": "sensor_wave",
                                'units': 'm',
                                'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/GTDHZZ01/'},
    'HUMIDITY': {'long_name': 'Relative humidity of the atmosphere',
                 'units': 'percent',
                 'sensor': 'sensor_meteorology',
                 'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/CRELZZ01/'},
    'PRESSURE_AIR': {'long_name': 'Pressure (spatial coordinate) exerted by the atmosphere',
                     'units': 'dbar',
                     'sensor': 'sensor_meteorology',
                     'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/CAPBZZ01/'},
    'TEMP_AIR': {'long_name': 'Temperature of the atmosphere by dry bulb thermometer',
                 'units': 'Celsius',
                 'sensor': 'sensor_meteorology',
                 'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/CDTASS01/'},
    'WIND_SPEED': {'long_name': 'Speed of wind {wind speed} in the atmosphere by in-situ anemometer',
                   'units': 'm s-1',
                   'sensor': 'sensor_meteorology',
                   'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/EWSBSS01/'},
    'WIND_DIRECTION': {
        'long_name': 'Direction (from) of wind relative to True North {wind direction} in the atmosphere by in-situ anemometer',
        'sensor': 'sensor_meteorology',
        'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/EWDASS01/'},
    "LATITUDE": {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                 'long_name': 'Latitude north',
                 'observation_type': 'measured',
                 'platform': 'platform',
                 'reference': 'WGS84',
                 'standard_name': 'latitude',
                 'units': 'degrees_north',
                 'valid_max': 90,
                 'valid_min': -90,
                 'axis': 'Y',
                 'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/LAT/',
                 },
    "LONGITUDE":
        {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
         'long_name': 'Longitude east',
         'observation_type': 'measured',
         'platform': 'platform',
         'reference': 'WGS84',
         'standard_name': 'longitude',
         'units': 'degrees_east',
         'valid_max': 180,
         'valid_min': -180,
         'axis': 'X',
         'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/LON/',
         },
    "TIME":
        {
            'long_name': 'time of measurement',
            'observation_type': 'measured',
            'standard_name': 'time',
            'units': 'seconds since 1970-01-01 00:00:00 UTC',
            'calendar': "gregorian",
            'axis': 'T',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/AYMD/',
        },
    "CNDC": {
        'sensor': 'sensor_ctd',
        'long_name': 'Electrical conductivity of the water body by CTD',
        'observation_type': 'measured',
        'standard_name': 'sea_water_electrical_conductivity',
        'units': 'mS cm-1',
        'valid_max': 85.,
        'valid_min': 0.,
        'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/CNDC/',
    },
    "PRES": {
        'comment': 'ctd pressure sensor',
        'sensor': 'sensor_ctd',
        'long_name': 'Pressure (spatial coordinate) exerted by the water body by profiling pressure sensor',
        'observation_type': 'measured',
        'positive': 'down',
        'reference_datum': 'sea-surface',
        'standard_name': 'sea_water_pressure',
        'units': 'dbar',
        'valid_max': 2000,
        'valid_min': 0,
        'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/PRES'
    },
    "PSAL": {'long_name': 'water salinity',
             'standard_name': 'sea_water_practical_salinity',
             'units': '1e-3',
             'comment': 'Practical salinity of the water body by CTD and computation using UNESCO 1983 algorithm',
             'sources': 'CNDC, TEMP, PRES',
             'observation_type': 'calculated',
             'sensor': 'sensor_ctd',
             'valid_max': 40,
             'valid_min': 0,
             'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/PSAL/'
             },
    "TEMP":
        {
            'long_name': 'Temperature of the water body by CTD ',
            'sensor': 'sensor_ctd',
            'observation_type': 'measured',
            'standard_name': 'sea_water_temperature',
            'units': 'Celsius',
            'valid_max': 42,
            'valid_min': -5,
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/TEMP/',
        },
    "PITCH":
        {
            'long_name': 'Orientation (pitch) of measurement platform by inclinometer',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/PITCH/',
            'units': 'degrees',
        },
    "ROLL":
        {
            'long_name': 'Orientation (roll angle) of measurement platform by inclinometer',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/ROLL/',
            'units': 'degrees',
        },
    "HEADING":
        {
            'long_name': 'Orientation (horizontal relative to magnetic north) of measurement platform {heading} by compass',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/HEADING/',
            'units': 'degrees',
        },
}

sensor_vocabs = {
    "RBR Legato3 CTD": {
        'sensor_type': 'CTD',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/130/',
        'sensor_maker': 'RBR',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0049/',
        'sensor_model': 'RBR Legato3 CTD',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1745/',
        'long_name': 'RBR Legato3 CTD',
    },
    "Gill Instruments GMX560": {
        'sensor_type': 'meteorological packages',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/102/',
        'sensor_maker': 'Gill Instruments',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0121/',
        'sensor_model': 'Gill Instruments MaxiMet Marine GMX560 weather station',
        'sensor_model_vocabulary': 'http://vocab.nerc.ac.uk/collection/L22/current/TOOL2098/',
        'long_name': 'Gill Instruments MaxiMet Marine GMX560 weather station',
    },
    "Datawell MOSE-G1000": {
        'sensor_type': 'wave recorders',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/110/',
        'sensor_maker': 'Datawell B.V.',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0124/',
        'sensor_model': 'Datawell MOSE-G1000',
        'sensor_model_vocabulary': 'Datawell MOSE-G1000',
        'long_name': 'Datawell MOSE-G1000',
    },
}

sensors = {
    "sensor_ctd": {"sensor": "RBR Legato3 CTD",
                   'serial_number': 207496,
                   'calibration_date': '2021-06-22'
                   },
    "sensor_meteorology": {"sensor": "Gill Instruments GMX560",
                           'serial_number': 24080013,
                           },
    "sensor_wave": {"sensor": "Datawell MOSE-G1000",
                    'serial_number': 'unknown',
                    },
}


def add_sensors(ds):
    for sensor_id, serial_dict in sensors.items():
        sensor_dict = sensor_vocabs[serial_dict['sensor']]
        for key, item in serial_dict.items():
            if key == 'sensor':
                continue
            sensor_dict[key] = item
        ds.attrs[sensor_id] = str(sensor_dict)
    return ds


def export_dataset():
    df = pd.read_parquet("data_out/delayed.pqt")
    ds = xr.Dataset()
    time_attr = {"name": "time"}
    ds['time'] = ('time', df.index, time_attr)

    for col_name in list(df):
        if col_name in clean_names.keys():
            name = clean_names[col_name]
            ds[name] = ('time', df[col_name], attrs_dict[name])
        else:
            print(f"fail for {col_name}")
    # cut dataset down to active deployed period
    start = "2024-05-29T09:00:00"
    end = "2024-07-28T06:00:00"
    ds = ds.sel(time=slice(start, end))
    ds.attrs = get_attrs(ds)
    ds = add_sensors(ds)
    ds.attrs["variables"] = list(ds.variables)
    ds['trajectory'] = xr.DataArray(1, attrs={"cf_role": "trajectory_id"})
    ds.to_netcdf(f"data_out/{ds.attrs['id']}.nc")
    #ds = ds.sel(time=slice(start, "2024-05-30T09:00:00"))
    ds.to_netcdf(f"/home/callum/Documents/erddap/local_dev/erddap-gold-standard/datasets/{ds.attrs['id']}.nc")


if __name__ == '__main__':
    # parse_nrt()
    # parse_legato()
    # merge_mose()
    # merge_gmx560()
    # merge_sensors()
    export_dataset()
