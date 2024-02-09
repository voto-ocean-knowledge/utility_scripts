import xarray as xr
from utilities import encode_times_og1


def convert_to_og1(ds):
    """
    Based on example by Jen Seva https://github.com/OceanGlidersCommunity/OG-format-user-manual/pull/136/files
    Using variable names from OG1 vocab https://vocab.nerc.ac.uk/collection/OG1/current/
    Using units from collection P07 http://vocab.nerc.ac.uk/collection/P07/current/
    e.g. mass_concentration_of_chlorophyll_a_in_sea_water from https://vocab.nerc.ac.uk/collection/P07/current/CF14N7/
    :param ds: dataset to convert
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

        if "instruments" in att.keys():
            PARAMETER_SENSOR.append(att["instruments"])
        elif "instrument" not in att.keys():
            PARAMETER_SENSOR.append("glider")
        else:
            PARAMETER_SENSOR.append(att["instrument"].split("_")[-1])

        PARAMETER_UNITS.append(att["units"])
    dsa = xr.Dataset()
    for var_name in list(ds) + list(ds.coords):
        if "_QC" in var_name:
            continue
        dsa[var_name] = ('N_MEASUREMENTS', ds[var_name].values, ds[var_name].attrs)
        qc_name = f'{var_name}_QC'
        if qc_name in list(ds):
            dsa[qc_name] = ('N_MEASUREMENTS', ds[qc_name].values, ds[qc_name].attrs)
            dsa[qc_name].attrs['long_name'] = f'{dsa[var_name].attrs["long_name"]} Quality Flag'
    dsa = dsa.set_coords(('TIME', 'LATITUDE', 'LONGITUDE', 'DEPTH'))

    dsa["PARAMETER"] = ("N_PARAMETERS", PARAMETER, {})
    dsa["PARAMETER_SENSOR"] = ("N_PARAMETERS", PARAMETER_SENSOR, {})
    dsa["PARAMETER_UNITS"] = ("N_PARAMETERS", PARAMETER_UNITS, {})
    dsa.attrs = ds.attrs
    dsa = encode_times_og1(dsa)
    return dsa


new_names = {
    "latitude": "LATITUDE",
    'longitude': 'LONGITUDE',
    'time': 'TIME',
    'depth': 'DEPTH',
    'pressure': 'PRES',
    'conductivity': 'CNDC',
    'oxygen_concentration': 'DOXY',
    'chlorophyll_concentration': 'CHLA',
    'temperature': 'TEMP',
    'salinity': 'PSAL',
    'density': 'DENSITY',
}

attrs_dict = {
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
    "TIME":
        {
            'long_name': 'Time',
            'observation_type': 'measured',
            'standard_name': 'time',
            'units': 'seconds since 1970-01-01 00:00:00 UTC',
            'calendar': "julian",
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
            'instrument': 'instrument_ctd',
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
        'instrument': 'instrument_ctd',
        'long_name': 'Electrical conductivity of the water body by CTD',
        'observation_type': 'measured',
        'standard_name': 'sea_water_electrical_conductivity',
        'units': 'mS cm^{-1}',
        'valid_max': '85.',
        'valid_min': '0.',
        'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/CNDC/',
    },
    "PRES": {
        'comment': 'ctd pressure sensor',
        'instrument': 'instrument_ctd',
        'long_name': 'Pressure (spatial coordinate) exerted by the water body by profiling pressure sensor and correction to read zero at sea level',
        'observation_type': 'measured',
        'positive': 'down',
        'reference_datum': 'sea-surface',
        'standard_name': 'sea_water_pressure',
        'units': 'dbar',
        'valid_max': '1000',
        'valid_min': '0',
        'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/PRES'
    },
    "PSAL": {'long_name': 'water salinity',
             'standard_name': 'sea_water_practical_salinity',
             'units': '1e-3',
             'comment': 'Practical salinity of the water body by CTD and computation using UNESCO 1983 algorithm',
             'sources': 'CNDC, TEMP, PRES',
             'observation_type': 'calculated',
             'instrument': 'instrument_ctd',
             'valid_max': '40.0',
             'valid_min': '0.0',
             'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/PSAL/'
             },
    "TEMP":
        {
            'long_name': 'Temperature of the water body by CTD ',
            'observation_type': 'measured',
            'standard_name': 'sea_water_temperature',
            'units': 'Celsius',
            'valid_max': '42',
            'valid_min': '-5',
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/TEMP/',

        },
    "DENSITY": {'long_name': 'The mass of a unit volume of any body of fresh or salt water',
                'standard_name': 'sea_water_density',
                'units': 'kg m-3',
                'comment': 'raw, uncorrected salinity',
                'observation_type': 'calculated',
                'sources': 'salinity temperature pressure',
                'valid_min': '1000.0',
                'valid_max': '1040.0',
                'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/DENSITY/'
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
    return dsa


if __name__ == '__main__':
    dsn = xr.open_dataset("/data/data_l0_pyglider/nrt/SEA76/M19/timeseries/mission_timeseries.nc")
    dsn = standardise_og10(dsn)
    dsn = convert_to_og1(dsn)
    dsn.to_netcdf("new.nc")
