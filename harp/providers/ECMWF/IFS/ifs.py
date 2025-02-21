# # standard library imports
# from typing import Callable
# from datetime import date
# from pathlib import Path

# # third party imports
# import xarray as xr
# import pandas as pd
# import numpy as np
# from core.static import interface
# from core import log

# # sub package imports
# from harp.baseprovider import BaseProvider
# from .ifs_layout import 
# from harp.providers import cdsapi_parser 

# from harp.utils import wrap, center_longitude


# class ERA5(BaseProvider):
#     """
#     Ancillary data provider using ERA5

#     - model: valid ERA5 models are listed in ERA5.models object
#     - directory: local folder path, where to download files 
#     - nomenclature_file: local file path to a nomenclature CSV to be used by the nomenclature module
#     - no_std: bypass the standardization to the nomenclature module, keeping the dataset as provided
#     """
    
#     models = ERA5_Models
    
#     def standardize(self, ds: xr.Dataset) -> xr.Dataset:
#         """
#         Open an ERA5 file and format it for consistency
#         with the other ancillary data sources
#         """
        
#         ds = self.names.rename_dataset(ds) # rename dataset according to nomenclature module
        
#         # new CDS longitudes E [0, 360]
#         ds = center_longitude(ds) # center longitude to 0 -> [-180, 180] 
        
#         lons = ds.longitude.values # if full map, make data a circle for proper interpolation
#         if np.isclose(np.min(lons), -180.0) and np.max(lons) > 179.0 and not np.isclose(np.max(lons), 180.0): 
#             ds = wrap(ds, 'longitude', -180, 180)
        
#         return ds
    
#     def __init__(self, model: Callable, directory: Path, nomenclature_file=None, offline: bool=False, verbose: bool=True, no_std: bool=False):
        
#         name = 'ERA5'
#         # call superclass constructor 
#         BaseProvider.__init__(self, name=name, model=model, directory=directory, nomenclature_file=nomenclature_file, 
#                               offline=offline, verbose=verbose, no_std=no_std)
        
#         self.client = None # cdsapi 

#         # ERA5 Reanalysis nomenclature (ads name: short name, etc..)
#         era5_csv_file = Path(__file__).parent / 'era5.csv' # file path relative to the module
#         self.model_specs = pd.read_csv(Path(era5_csv_file).resolve(), skipinitialspace=True)               # read csv file
#         self.model_specs = self.model_specs.apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # remove trailing whitespaces
#         self.model_specs = self.model_specs[~self.model_specs['name'].astype(str).str.startswith('#')]                      # remove comment lines
        
#         # get credentials from .cdsapirc file
#         self.cdsapi_cfg = self._parse_cdsapirc()
        
#         # computables variables and their requirements
#         # functions needs to have the same parameters: (ds, new_var)
#         self.computables['#windspeed'] = (ERA5.compute_windspeed, ['u10', 'v10'])
        
#         # init cdsapi client
#         self.client = cdsapi.Client(url=self.cdsapi_cfg['url'], key=self.cdsapi_cfg['key'])
        
#     # ----{ computed variables }----
#     @staticmethod
#     def compute_windspeed(ds, new_var) -> xr.Dataset:
#         ds[new_var] = np.sqrt(ds.u10**2 + ds.v10**2)
#         return ds
#     # ------------------------------

#     @interface
#     def download(self, variables: list[str], d: date, area: None|list=None) -> Path:
#         """
#         Download ERA5 model for the given date
        
#         - variables: list of strings of the CAMS variables short names to download ex: ['gtco3', 'aod550', 'parcs']
#         - d: date of the data (not datetime)
#         - area: [90, -180, -90, 180] → [north, west, south, east]
#         """
        
#         shortnames = variables # true if no_std is true

#         # prepare variable attributes
#         if self.no_std:
#             for var in variables: # verify var nomenclature has been defined in csv, beforehand
#                 self.names.assert_shortname_is_defined(var)
#             self.cds_variables = [self.get_cds_name(var) for var in variables] # get ads name equivalent from short name
#         else:
#             shortnames = [self.names.get_shortname(var) for var in variables]
#             self.cds_variables = [self.get_cds_name(var) for var in shortnames] 
            
#         # transform function name to extract only the acronym
#         acronym = ''.join([i[0] for i in self.model.__name__.upper().split('_')])
#         # ex: reanalysis_single_level → 'ESL'            

#         # output file path
#         file_path = self.directory / Path(self._get_filename(shortnames, d, acronym, area))    # get path
        
#         if not file_path.exists():  # download if not already present
#             if self.offline:        # download needed but deactivated → raise error
#                 raise ResourceWarning(f'Could not find local file {file_path}, offline mode is set')
            
#             if self.verbose: 
#                 log.info(f'downloading: {file_path.name}')
#             self.model(self, file_path, d, area) # download file
            
#         elif self.verbose: # elif → file already exists
#             log.info(f'found locally: {file_path.name}')
                
#         return file_path
        

#     def get_cds_name(self, short_name):
#         """
#         Returns the variable's ADS name (used to querry the Atmospheric Data Store)
#         """
#         # verify beforehand that the var has been properly defined
#         if short_name not in list(self.model_specs['short_name'].values):
#             raise KeyError(f'Could not find short_name {short_name} in csv file')
        
#         return self.model_specs[self.model_specs['short_name'] == short_name]['cds_name'].values[0]

    
#     def _parse_cdsapirc(self):
#         """
#         after retrieval the function sets attributes cdsapi_url and cdsapi_key to
#         pass as parameter in the Client constructor
#         """
#         # taken from ECMWF's cdsapi code
#         dotrc = os.environ.get("CDSAPI_RC", os.path.expanduser("~/.cdsapirc"))
#         config = cdsapi_parser.read_config('cds', dotrc) 
#         # save the credentials as attributes
#         return config