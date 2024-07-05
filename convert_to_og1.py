import pandas as pd
import numpy as np
import xarray as xr
from utilities import encode_times_og1, set_best_dtype

sensor_vocabs = {
    "RBR legato CTD": {
        'sensor_type': 'CTD',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/130/',
        'sensor_maker': 'RBR',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0049/',
        'sensor_model': 'RBR Legato3 CTD',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1745/',
        'long_name': 'RBR Legato3 CTD',
    },
    "Wetlabs FLBBCD": {
        'sensor_type': 'fluorometers',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/113/',
        'sensor_maker': 'WET Labs',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0026/',
        'sensor_model': 'WET Labs {Sea-Bird WETLabs} ECO FLBBCD scattering fluorescence sensor',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1141/',
        'long_name': 'WET Labs ECO-FLBBCD',
    },
    "Nortek AD2CP": {
        'sensor_type': 'ADVs and turbulence probes',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/384/',
        'sensor_maker': 'Nortek',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0068/',
        'sensor_model': 'Nortek Glider1000 AD2CP Acoustic Doppler Current Profiler',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1774/',
        'long_name': 'Nortek AD2CP',
    },
    "JFE Advantech AROD_FT": {
        'sensor_type': 'dissolved gas sensors',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/351/',
        'sensor_maker': 'JFE Advantech',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0053/',
        'sensor_model': 'JFE Advantech Rinko FT ARO-FT oxygen sensor',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1783/',
        'long_name': 'JFE Rinko ARO-FT',
    },
}

variables_sensors = {'CNDC': 'CTD',
                     'DOXY': 'dissolved gas sensors',
                     'PRES': 'CTD',
                     'PSAL': 'CTD',
                     'TEMP': 'CTD',
                     'BBP700': 'fluorometers',
                     'CHLA': 'fluorometers',
                     'PRES_ADCP': 'ADVs and turbulence probes',
                     }


def add_sensors(ds, dsa):
    attrs = ds.attrs
    sensors = []
    for key, var in attrs.items():
        if type(var) != str:
            continue
        if '{' not in var:
            continue
        if type(eval(var)) == dict:
            sensors.append(key)

    sensor_name_type = {}
    for instr in sensors:
        attr_dict = eval(attrs[instr])
        if attr_dict['make_model'] not in sensor_vocabs.keys():
            print(attr_dict['make_model'], "not found")
            continue
        var_dict = sensor_vocabs[attr_dict['make_model']]
        if 'serial' in attr_dict.keys():
            var_dict['serial_number'] = str(attr_dict['serial'])
            var_dict['long_name'] += f":{str(attr_dict['serial'])}"
        for var_name in ['calibration_date', 'calibration_parameters']:
            if var_name in attr_dict.keys():
                var_dict[var_name] = str(attr_dict[var_name])
        da = xr.DataArray(attrs=var_dict)
        sensor_var_name = f"sensor_{var_dict['sensor_type']}_{var_dict['serial_number']}".upper().replace(' ', '_')
        dsa[sensor_var_name] = da
        sensor_name_type[var_dict['sensor_type']] = sensor_var_name

    for key, var in attrs.copy().items():
        if type(var) != str:
            continue
        if '{' not in var:
            continue
        if type(eval(var)) == dict:
            attrs.pop(key)
    ds.attrs = attrs

    for key, sensor_type in variables_sensors.items():
        if key in dsa.variables:
            instr_key = sensor_name_type[sensor_type]
            dsa[key].attrs['sensor'] = instr_key

    return ds, dsa


def convert_to_og1(ds, parameters=False, num_vals=10, postscript='R'):
    """
    Based on example by Jen Seva https://github.com/OceanGlidersCommunity/OG-format-user-manual/pull/136/files
    Using variable names from OG1 vocab https://vocab.nerc.ac.uk/collection/OG1/current/
    Using units from collection P07 http://vocab.nerc.ac.uk/collection/P07/current/
    e.g. mass_concentration_of_chlorophyll_a_in_sea_water from https://vocab.nerc.ac.uk/collection/P07/current/CF14N7/
    :param ds: dataset to convert
    :param parameters: True if you want to set the (optional) extra dimension PARAMS
    :return: converted dataset
    """
    PARAMETER = []
    PARAMETER_SENSOR = []
    PARAMETER_UNITS = []

    for var_name in list(ds):
        if "QC" in var_name:
            continue
        var = ds[var_name]
        att = var.attrs
        PARAMETER.append(var_name)
        if "units" not in att.keys():
            att["units"] = "None"
        if "sensors" in att.keys():
            PARAMETER_SENSOR.append(att["sensors"])
        elif "sensor" not in att.keys():
            PARAMETER_SENSOR.append("glider")
        else:
            PARAMETER_SENSOR.append(att["sensor"].split("_")[-1])
        PARAMETER_UNITS.append(att["units"])
    dsa = xr.Dataset()
    for var_name in list(ds) + list(ds.coords):
        if "_QC" in var_name:
            continue
        dsa[var_name] = ('N_MEASUREMENTS', ds[var_name].values[:num_vals], ds[var_name].attrs)
        qc_name = f'{var_name}_QC'
        if qc_name in list(ds):
            dsa[qc_name] = ('N_MEASUREMENTS', ds[qc_name].values[:num_vals].astype("int8"), ds[qc_name].attrs)
            dsa[qc_name].attrs['long_name'] = f'{dsa[var_name].attrs["long_name"]} Quality Flag'
            dsa[qc_name].attrs['standard_name'] = 'status_flag'
            dsa[qc_name].attrs['flag_values'] = np.array((1, 2, 3, 4, 9)).astype("int8")
            dsa[qc_name].attrs['flag_meanings'] = 'GOOD UNKNOWN SUSPECT FAIL MISSING'
            dsa[var_name].attrs['ancillary_variables'] = qc_name
    if "time" in str(dsa.TIME.dtype):
        var_name = "TIME"
        dsa[var_name].values = dsa[var_name].values.astype(float)
        if np.nanmean(dsa[var_name].values) > 1e12:
            dsa[var_name].values = dsa[var_name].values / 1e9
    dsa = dsa.set_coords(('TIME', 'LATITUDE', 'LONGITUDE', 'DEPTH'))
    for vname in ['LATITUDE', 'LONGITUDE', 'TIME']:
        dsa[f'{vname}_GPS'] = dsa[vname].copy()
        dsa[f'{vname}_GPS'].values[dsa['PHASE'].values != 119] = np.nan
        dsa[f'{vname}_GPS'].attrs['long_name'] = f'{vname.lower()} of each GPS location'
    seaex_phase = dsa['PHASE'].values
    standard_phase = np.zeros(len(seaex_phase))
    standard_phase[seaex_phase == 115] = 3
    standard_phase[seaex_phase == 116] = 3
    standard_phase[seaex_phase == 119] = 3
    standard_phase[seaex_phase == 110] = 5
    standard_phase[seaex_phase == 118] = 5
    standard_phase[seaex_phase == 100] = 2
    standard_phase[seaex_phase == 117] = 1
    dsa['PHASE'].values = standard_phase
    dsa['PHASE'].attrs = {'long_name': "behavior of the glider at sea",
                          'phase_vocabulary': 'https://github.com/OceanGlidersCommunity/OG-format-user-manual/blob/main/vocabularyCollection/phase.md'}
    ds, dsa = add_sensors(ds, dsa)
    attrs = ds.attrs
    if parameters:
        dsa["PARAMETER"] = ("N_PARAMETERS", PARAMETER, {})
        dsa["PARAMETER_SENSOR"] = ("N_PARAMETERS", PARAMETER_SENSOR, {})
        dsa["PARAMETER_UNITS"] = ("N_PARAMETERS", PARAMETER_UNITS, {})
    ts = pd.to_datetime(ds.time_coverage_start).strftime('%Y%m%dT%H%M')
    attrs['id'] = f"sea{str(attrs['glider_serial']).zfill(3)}_{ts}_{postscript}"
    attrs['title'] = 'OceanGliders example file for SeaExplorer data'
    attrs['platform'] = 'sub-surface gliders'
    attrs['platform_vocabulary'] = 'https://vocab.nerc.ac.uk/collection/L06/current/27/'
    attrs['contributor_email'] = 'callum.rollo@voiceoftheocean.org, louise.biddle@voiceoftheocean.org, , , , , , '
    attrs['contributor_role_vocabulary'] = 'https://vocab.nerc.ac.uk/collection/W08/'
    attrs['contributor_role'] = 'Data scientist, PI, Operator, Operator, Operator, Operator, Operator, Operator,'
    attrs['contributing_institutions'] = 'Voice of the Ocean Foundation'
    attrs['contributing_institutions_role'] = 'Operator'
    attrs['contributing_institutions_role_vocabulary'] = 'https://vocab.nerc.ac.uk/collection/W08/current/'
    attrs['date_modified'] = attrs['date_created']
    attrs['agency'] = 'Voice of the Ocean'
    attrs['agency_role'] = 'contact point'
    attrs['agency_role_vocabulary'] = 'https://vocab.nerc.ac.uk/collection/C86/current/'
    attrs['data_url'] = f"https://erddap.observations.voiceoftheocean.org/erddap/tabledap/{attrs['dataset_id']}"
    attrs['rtqc_method'] = "IOOS QC QARTOD https://github.com/ioos/ioos_qc"
    attrs['rtqc_method_doi'] = "None"
    attrs['featureType'] = 'trajectory'
    attrs['Conventions'] = 'CF-1.10, OG-1.0'
    attrs['comment'] = 'Dataset for demonstration purposes only. Original dataset truncated for the sake of simplicity'
    attrs['start_date'] = attrs['time_coverage_start']
    dsa.attrs = attrs
    dsa['TRAJECTORY'] = xr.DataArray(ds.attrs['id'], attrs={'cf_role': 'trajectory_id', 'long_name': 'trajectory name'})
    dsa['WMO_IDENTIFIER'] = xr.DataArray(ds.attrs['wmo_id'], attrs={'long_name': 'wmo id'})
    dsa['PLATFORM_MODEL'] = xr.DataArray(ds.attrs['glider_model'], attrs={'long_name': 'model of the glider',
                                                                          'platform_model_vocabulary': "None"})
    dsa['PLATFORM_SERIAL_NUMBER'] = xr.DataArray(f"sea{ds.attrs['glider_serial'].zfill(3)}",
                                                 attrs={'long_name': 'glider serial number'})
    dsa['DEPLOYMENT_TIME'] = np.nanmin(dsa.TIME.values)
    dsa['DEPLOYMENT_TIME'].attrs = {'long_name': 'date of deployment',
                                    'standard_name': 'time',
                                    'units': 'seconds since 1970-01-01T00:00:00Z',
                                    'calendar': 'gregorian'}
    dsa['DEPLOYMENT_LATITUDE'] = dsa.LATITUDE.values[0]
    dsa['DEPLOYMENT_LATITUDE'].attrs = {'long_name': 'latitude of deployment'}
    dsa['DEPLOYMENT_LONGITUDE'] = dsa.LONGITUDE.values[0]
    dsa['DEPLOYMENT_LONGITUDE'].attrs = {'long_name': 'longitude of deployment'}
    dsa = encode_times_og1(dsa)
    dsa = set_best_dtype(dsa)
    return dsa


new_names = {
    "latitude": "LATITUDE",
    'longitude': 'LONGITUDE',
    'time': 'TIME',
    'depth': 'DEPTH',
    'pressure': 'PRES',
    'conductivity': 'CNDC',
    'oxygen_concentration': 'DOXY',
    'chlorophyll': 'CHLA',
    'temperature': 'TEMP',
    'salinity': 'PSAL',
    'density': 'DENSITY',
    'profile_index': 'PROFILE_NUMBER',
    'nav_state': 'PHASE',
    'adcp_Pressure': 'PRES_ADCP',
    'particulate_backscatter': 'BBP700',
}

attrs_dict = {
    "LATITUDE": {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
                 'long_name': 'latitude of each measurement and GPS location',
                 'observation_type': 'measured',
                 'platform': 'platform',
                 'reference': 'WGS84',
                 'standard_name': 'latitude',
                 'units': 'degrees_north',
                 'valid_max': 90,
                 'valid_min': -90,
                 'axis': 'Y',
                 },
    "LONGITUDE":
        {'coordinate_reference_frame': 'urn:ogc:crs:EPSG::4326',
         'long_name': 'longitude of each measurement and GPS location',
         'observation_type': 'measured',
         'platform': 'platform',
         'reference': 'WGS84',
         'standard_name': 'longitude',
         'units': 'degrees_east',
         'valid_max': 180,
         'valid_min': -180,
         'axis': 'X',
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
    'DEPTH':
        {
            'source': 'pressure',
            'long_name': 'glider depth',
            'standard_name': 'depth',
            'units': 'm',
            'comment': 'from science pressure and interpolated',
            'sensor': 'sensor_ctd',
            'observation_type': 'calculated',
            'accuracy': 1,
            'precision': 2,
            'resolution': 0.02,
            'platform': 'platform',
            'valid_min': 0,
            'valid_max': 2000,
            'reference_datum': 'surface',
            'positive': 'down'
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
        'long_name': 'Pressure (spatial coordinate) exerted by the water body by profiling pressure sensor and correction to read zero at sea level',
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
            'observation_type': 'measured',
            'standard_name': 'sea_water_temperature',
            'units': 'Celsius',
            'valid_max': 42,
            'valid_min': -5,
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/TEMP/',

        },
    "DENSITY": {'long_name': 'The mass of a unit volume of any body of fresh or salt water',
                'standard_name': 'sea_water_density',
                'units': 'kg m-3',
                'comment': 'raw, uncorrected salinity',
                'observation_type': 'calculated',
                'sources': 'salinity temperature pressure',
                'valid_min': 1000,
                'valid_max': 1040,
                'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/DENSITY/'
                },
    "PROFILE_NUMBER": {'long_name': 'profile index',
                       'units': '1'
                       },
    'PHASE': {'long_name': 'behavior of the glider at sea',
              'comment': 'This is the variable NAV_STATE from the SeaExplorer nav file',
              'units': '1'},
    "PRES_ADCP": {
        'comment': 'adcp pressure sensor',
        'sensor': 'sensor_adcp',
        'long_name': 'Pressure (spatial coordinate) exerted by the water body by profiling pressure sensor and correction to read zero at sea level',
        'observation_type': 'measured',
        'positive': 'down',
        'reference_datum': 'sea-surface',
        'standard_name': 'sea_water_pressure',
        'units': 'dbar',
        'valid_max': 2000,
        'valid_min': 0,
        'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/PRES'
    },
    "BBP700":
        {
            'long_name': 'Particle backscattering at 700 nanometers.',
            'observation_type': 'calculated',
            'units': 'm-1',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/BBP700/',
            'processing': 'Particulate backscatter b_bp calculated following methods in the Ocean Observatories Initiative document DATA PRODUCT SPECIFICATION FOR OPTICAL BACKSCATTER (RED WAVELENGTHS) Version 1-05 Document Control Number 1341-00540 2014-05-28. Downloaded from https://oceanobservatories.org/wp-content/uploads/2015/10/1341-00540_Data_Product_SPEC_FLUBSCT_OOI.pdf',
        },

}


def standardise_og10(ds):
    dsa = xr.Dataset()
    dsa.attrs = ds.attrs
    for var_name in list(ds) + list(ds.coords):
        if var_name in new_names.keys():
            name = new_names[var_name]
            dsa[name] = ('time', ds[var_name].values, attrs_dict[name])
            qc_name = f'{var_name}_qc'
            if qc_name in list(ds):
                dsa[f'{name}_QC'] = ('time', ds[qc_name].values, ds[qc_name].attrs)
                dsa[name].attrs['ancillary_variables'] = f'{name}_QC'
    dsa = set_best_dtype(dsa)
    return dsa


if __name__ == '__main__':
    dsn = xr.open_dataset("/data/data_l0_pyglider/nrt/SEA76/M19/timeseries/mission_timeseries.nc")
    dsn = standardise_og10(dsn)
    dsn = convert_to_og1(dsn)
    dsn.to_netcdf("new.nc")
