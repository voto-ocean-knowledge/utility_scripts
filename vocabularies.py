standard_names = {
    "latitude": "LATITUDE",
    'longitude': 'LONGITUDE',
    'time': 'TIME',
    'pitch': 'PITCH',
    'roll': 'ROLL',
    'heading': 'HEADING',
    'depth': 'DEPTH',
    'pressure': 'PRES',
    'conductivity': 'CNDC',
    'oxygen_concentration': 'DOXY',
    'chlorophyll': 'CHLA',
    'temperature': 'TEMP',
    'salinity': 'PSAL',
    'density': 'DENSITY',
    'profile_index': 'PROFILE_NUMBER',
    'adcp_Pressure': 'PRES_ADCP',
    'particulate_backscatter': 'BBP700',
    'backscatter_scaled': 'BBP700',
    'backscatter_raw': 'RBBP700',
    'potential_temperature': 'THETA',
    'down_irradiance_380': 'ED380',
    'down_irradiance_490': 'ED490',
    'downwelling_PAR': 'DPAR',
    'temperature_oxygen': 'TEMP_OXYGEN',
    'potential_density': 'POTDENS0',
    'chlorophyll_raw': 'FLUOCHLA',
    'ad2cp_pitch': 'AD2CP_PITCH',
    'ad2cp_roll': 'AD2CP_ROLL',
    'ad2cp_heading': 'AD2CP_HEADING',
    'ad2cp_time': 'AD2CP_TIME',
    'ad2cp_pressure': 'AD2CP_PRES',
    'turbidity': 'TURB',
    'cdom': 'CDOM',
    'cdom_raw': 'FLUOCDOM',
    'phycoerythrin': 'PHYC',
    'phycoerythrin_raw': 'FLUOPHYC',
    'tke_dissipation_shear_1': 'EPSIFY01',
    'tke_dissipation_shear_2': 'EPSIFY02',
}

vocab_attrs = {
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
    "AD2CP_TIME":
        {
            'long_name': 'time of measurement',
            'observation_type': 'measured',
            'standard_name': 'time',
            'units': 'seconds since 1970-01-01 00:00:00 UTC',
            'calendar': "gregorian",
            'axis': 'T',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/AYMD/',
            'comment': 'measured by AD2CP',
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
        'long_name': 'Pressure (spatial coordinate) exerted by the water body by profiling pressure sensor and '
                     'correction to read zero at sea level',
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
    "THETA":
        {
            'long_name': 'Potential temperature of the water body by computation using UNESCO 1983 algorithm.',
            'observation_type': 'calculated',
            'sources': 'salinity temperature pressure',
            'standard_name': 'sea_water_potential_temperature',
            'units': 'Celsius',
            'valid_max': 42,
            'valid_min': -5,
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/THETA/',
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
    "AD2CP_PRES": {
        'comment': 'adcp pressure sensor',
        'sensor': 'sensor_adcp',
        'long_name': 'Pressure (spatial coordinate) exerted by the water body by profiling pressure sensor and '
                     'correction to read zero at sea level',
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
            'processing': 'Particulate backscatter bbp calculated following methods in the Ocean Observatories '
                          'Initiative document DATA PRODUCT SPECIFICATION FOR OPTICAL BACKSCATTER (RED WAVELENGTHS) '
                          'Version 1-05 Document Control Number 1341-00540 2014-05-28. Downloaded from '
                          'https://oceanobservatories.org/wp-content/uploads/2015/10/1341'
                          '-00540_Data_Product_SPEC_FLUBSCT_OOI.pdf',
        },
    "RBBP700":
        {
            'long_name': 'Raw signal from backscattering sensor.',
            'observation_type': 'observed',
            'units': '1',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/RBBP700/',
        },
    'ED380': {'average_method': 'geometric mean',
              'long_name': 'The vertical component of light at 380nm wavelength travelling downwards',
              'observation_type': 'measured',
              'standard_name': '380nm_downwelling_irradiance',
              'units': 'W m-2 nm-1',
              'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/ED380/',
              },
    'ED490': {'average_method': 'geometric mean',
              'long_name': 'The vertical component of light at 490nm wavelength travelling downwards',
              'observation_type': 'measured',
              'standard_name': '490nm_downwelling_irradiance',
              'units': 'W m-2 nm-1',
              'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/ED490/',
              },
    "DPAR":
        {
            'long_name': 'Downwelling vector irradiance as energy of electromagnetic radiation (PAR wavelengths) in '
                         'the water body by cosine-collector radiometer.',
            'observation_type': 'measured',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/DPAR/',
        },
    "PITCH":
        {
            'long_name': 'Orientation (pitch) of measurement platform by inclinometer',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/PITCH/',
        },
    "ROLL":
        {
            'long_name': 'Orientation (roll angle) of measurement platform by inclinometer',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/ROLL/',
        },
    "HEADING":
        {
            'long_name': 'Orientation (horizontal relative to magnetic north) of measurement platform {heading} by '
                         'compass',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/HEADING/',
        },
    "AD2CP_PITCH":
        {
            'long_name': 'Orientation (pitch) of measurement platform by inclinometer',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/PITCH/',
            'comment': 'measured by AD2CP',
        },
    "AD2CP_ROLL":
        {
            'long_name': 'Orientation (roll angle) of measurement platform by inclinometer',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/ROLL/',
            'comment': 'measured by AD2CP',
        },
    "AD2CP_HEADING":
        {
            'long_name': 'Orientation (horizontal relative to magnetic north) of measurement platform {heading} by '
                         'compass',
            'URI': 'https://vocab.nerc.ac.uk/collection/P02/current/HEADING/',
            'comment': 'measured by AD2CP',
        },
    "TEMP_OXYGEN":
        {
            'long_name': 'Temperature of the water body by CTD ',
            'observation_type': 'measured',
            'standard_name': 'sea_water_temperature',
            'comment': 'measured by oxygen optode',
            'units': 'Celsius',
            'valid_max': 42,
            'valid_min': -5,
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/TEMP/',
        },
    "POTDENS0":
        {
            'long_name': 'Potential density of water body at surface',
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/POTDENS0/',
        },
    "FLUOCHLA":
        {
            'long_name': 'Raw signal (counts) of instrument output by in-situ chlorophyll fluorometer',
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/FLUOCHLA/',
        },
    "TURB":
        {
            'long_name': 'Turbidity of water in the water body',
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/TURB/',
        },
    "TCPUCHLA":
        {
            'long_name': 'Turbidity of water in the water body',
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/TCPUCHLA/',
        },
    "CDOM":
        {
            'long_name': 'Concentration of coloured dissolved organic matter in sea water',
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/CDOM/',
        },
    "FLUOCDOM":
        {
            'long_name': 'Raw fluorescence from coloured dissolved organic matter sensor',
            'URI': 'https://vocab.nerc.ac.uk/collection/OG1/current/FLUOCDOM/',
        },
    "PHYC":
        {
            'long_name': 'Phycoerythrin concentration per unit volume of fresh or salt water.',
            'URI': 'http://vocab.nerc.ac.uk/collection/OG1/current/PHYC/',
        },
    "FLUOPHYC":
        {
            'long_name': 'Phycoerythrin signal from fluorescence sensor',
            'URI': 'http://vocab.nerc.ac.uk/collection/OG1/current/FLUOPHYC/',
        },
    "EPSIFY01":
        {
            'long_name': 'Log10 turbulent kinetic energy dissipation {epsilon} per unit volume of the water body by '
                         'turbulence profiler shear sensor',
            'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/EPSIFY01/',
        },
    "EPSIFY02":
        {
            'long_name': 'Log10 turbulent kinetic energy dissipation {epsilon} per unit volume of the water body by '
                         'turbulence profiler shear sensor',
            'URI': 'http://vocab.nerc.ac.uk/collection/P01/current/EPSIFY02/',
        },
}


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
    "Wetlabs FLBBPC": {
        'sensor_type': 'fluorometers',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/113/',
        'sensor_maker': 'WET Labs',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0026/',
        'sensor_model': 'WET Labs {Sea-Bird WETLabs} ECO Puck Triplet FLBBPC scattering fluorescence sensor',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1904/',
        'long_name': ' WET Labs ECO FLBBPC',
    },
    "Wetlabs FLBBPE": {
        'sensor_type': 'fluorometers',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/113/',
        'sensor_maker': 'WET Labs',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0026/',
        'sensor_model': 'WET Labs {Sea-Bird WETLabs} ECO Triplet sensor',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL0674/',
        'long_name': 'WET Labs ECO-FLBBCE',
    },
    "Wetlabs FLNTU": {
        'sensor_type': 'fluorometers',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/113/',
        'sensor_maker': 'WET Labs',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0026/',
        'sensor_model': 'WET Labs {Sea-Bird WETLabs} ECO FLNTU combined fluorometer and turbidity sensor',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL0215/',
        'long_name': ' WET Labs ECO FLNTU',
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
    "RBR coda TODO": {
        'sensor_type': 'dissolved gas sensors',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/351/',
        'sensor_maker': 'RBR',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0049/',
        'sensor_model': 'RBR Coda T.ODO Temperature and Dissolved Oxygen Sensor',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1783/',
        'long_name': 'RBR Coda T.ODO',
    },
    "SeaBird OCR504": {
        'sensor_type': 'radiometers',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/122/',
        'sensor_maker': 'Sea-Bird Scientific',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0013/',
        'sensor_model': 'Satlantic {Sea-Bird} OCR-504 multispectral radiometer',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL0625/',
        'long_name': 'Sea-Bird OCR-504 ',
    },
    "Seabird Deep SUNA": {
        'sensor_type': 'nutrient analysers',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/181/',
        'sensor_maker': 'Sea-Bird Scientific',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0013/',
        'sensor_model': 'Satlantic {Sea-Bird} Submersible Ultraviolet Nitrate Analyser V2 (SUNA V2) nutrient analyser series',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1562/',
        'long_name': 'Sea-Bird SUNA ',
    },
    "Franatech METS": {
        'sensor_type': 'dissolved gas sensors',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/351/',
        'sensor_maker': 'Franatech',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0303/',
        'sensor_model': 'Franatech METS Methane Sensor',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1244/',
        'long_name': 'Franatech METS Methane Sensor ',
    },
    "Biospherical MPE-PAR": {
        'sensor_type': 'radiometers',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/122/',
        'sensor_maker': 'Biospherical Instruments',
        'sensor_maker_vocabulary': 'http://vocab.nerc.ac.uk/collection/L35/current/MAN0028/',
        'sensor_model': 'Biospherical PAR sensor (UnSpec model)',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1254/',
        'long_name': 'Biospherical PAR sensor',
    },
    "Rockland Scientific MR1000G-RDL": {
        'sensor_type': 'microstructure sensors',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/184/',
        'sensor_maker': 'Rockland Scientific',
        'sensor_maker_vocabulary': 'http://vocab.nerc.ac.uk/collection/L35/current/MAN0022/',
        'sensor_model': 'Rockland MicroRider-1000',
        'sensor_model_vocabulary': 'http://vocab.nerc.ac.uk/collection/L22/current/TOOL1232/',
        'long_name': 'Rockland MicroRider-1000',
    },
    "Seabird SlocumCTD": {
        'sensor_type': 'CTD',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/130/',
        'sensor_maker': 'Sea-Bird Scientific',
        'sensor_maker_vocabulary': 'http://vocab.nerc.ac.uk/collection/L35/current/MAN0013/',
        'sensor_model': 'Sea-Bird Slocum Glider Payload {GPCTD} CTD',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1492/',
        'long_name': 'Sea-Bird Slocum CTD',
    },
    "SeaOWL UV-A": {
        'sensor_type': 'fluorometers',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/113/',
        'sensor_maker': 'WET Labs',
        'sensor_maker_vocabulary': 'https://vocab.nerc.ac.uk/collection/L35/current/MAN0026/',
        'sensor_model': 'WET Labs {Sea-Bird WETLabs} SeaOWL UV-A Sea Oil-in-Water Locator',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL1766/',
        'long_name': 'WET Labs SeaOWL',
    },
    "Seabird SBE43F": {
        'sensor_type': 'dissolved gas sensors',
        'sensor_type_vocabulary': 'https://vocab.nerc.ac.uk/collection/L05/current/351/',
        'sensor_maker': 'Sea-Bird Scientific',
        'sensor_maker_vocabulary': 'http://vocab.nerc.ac.uk/collection/L35/current/MAN0013/',
        'sensor_model': 'Sea-Bird SBE 43F Dissolved Oxygen Sensor',
        'sensor_model_vocabulary': 'https://vocab.nerc.ac.uk/collection/L22/current/TOOL0037/',
        'long_name': 'Sea-Bird SBE 43F',
    },
}