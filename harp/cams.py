# standard library imports
from typing import Callable
from datetime import date
from pathlib import Path

# third party imports
import xarray as xr
import pandas as pd
import numpy as np
import os
from core.static import interface
from core.floats import feq

# sub package imports
from harp.baseprovider import BaseProvider
from harp.cams_models import CAMS_Models
from harp import cdsapi_parser 
from harp.utils import wrap, center_longitude

class CAMS(BaseProvider):
    """
    Ancillary data provider for CAMS models

    - model: valid CAMS models are listed in CAMS.models object
    - directory: local folder path, where to download files 
    - nomenclature_file: local file path to a nomenclature CSV to be used by the nomenclature module
    - no_std: bypass the standardization to the nomenclature module, keeping the dataset as provided
    """
    
    models = CAMS_Models
    
    def standardize(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Open a CAMS file and standardize it for consistency
        with the other ancillary data sources
        """
        
        ds = self.names.rename_dataset(ds) # rename dataset according to nomenclature module
        
        # new ADS longitudes E [0, 360]
        ds = center_longitude(ds) # center longitude to 0 -> [-180, 180] 
        
        lons = ds.longitude.values # if full map, make data a circle for proper interpolation
        
        if np.min(lons) != -180.0 and feq(np.min(lons), -180.0, tol=1e-6):
            tresh = -(180 - 1e-6) # ~= -179.999999
            lons[lons <= tresh] = -180.0 # force value to be -180 if closer than 1e-6, necessary for wrap
            ds = ds.assign_coords(longitude = lons) # reassign new lons
        
        if feq(np.min(lons), -180.0) and np.max(lons) > 179.0 and not feq(np.max(lons), 180.0): 
            ds = wrap(ds, 'longitude', -180, 180)
        return ds
    
    
    def __init__(self, model: Callable, directory: Path, nomenclature_file=None, offline: bool=False, verbose: bool=True, no_std: bool=False):
        
        name = 'CAMS'
        # call superclass constructor 
        BaseProvider.__init__(self, name=name, model=model, directory=directory, nomenclature_file=nomenclature_file, 
                              offline=offline, verbose=verbose, no_std=no_std)
        
        self.client = None # cdsapi 
        
        # CAMS nomenclature (ads name: short name, etc..)
        cams_csv_file = Path(__file__).parent / 'cams.csv' # file path relative to the module
        self.model_specs = pd.read_csv(Path(cams_csv_file).resolve(), skipinitialspace=True)               # read csv file
        self.model_specs = self.model_specs.apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # remove trailing whitespaces
        self.model_specs = self.model_specs[~self.model_specs['name'].astype(str).str.startswith('#')] # remove comment lines
                
        # get credentials from .cdsapirc file
        self.cdsapi_cfg = self._parse_cdsapirc()
    
        # computables variables and their requirements
        # functions needs to have the same parameters: (ds, new_var)
        self.computables['#totangstr'] = (CAMS.compute_totangstr, ['aod469', 'aod670'])
        self.computables['#windspeed'] = (CAMS.compute_windspeed, ['u10', 'v10'])
        
    
    # ----{ computed variables }----
    @staticmethod
    def compute_totangstr(ds, new_var) -> xr.Dataset:
        ds[new_var] = -np.log(ds.aod469/ds.aod670) /  np.log(469.0/670.0)
        return ds
    
    @staticmethod
    def compute_windspeed(ds, new_var) -> xr.Dataset:
        ds[new_var] = np.sqrt(ds.u10**2 + ds.v10**2)
        return ds
    # ------------------------------
    
    @interface
    def download(self, variables: list[str], d: date, area: list|None=None) -> Path:
        """
        Download CAMS model for the given date
        
        https://ads.atmosphere.copernicus.eu/cdsapp#!/dataset/cams-global-atmospheric-composition-forecasts
        https://confluence.ecmwf.int/display/CKB/CAMS%3A+Global+atmospheric+composition+forecast+data+documentation#heading-Table1SinglelevelFastaccessparameterslastreviewedon02Aug2023
        
        - variables: list of strings of the CAMS variables short names to download ex: ['gtco3', 'aod550', 'parcs']
        - d: date of the data (not datetime)
         - area: [90, -180, -90, 180] -> [north, west, south, east]
        """
        
        shortnames = variables.copy() # true if no_std is true
        
        # prepare variable attributes
        if self.no_std:
            for var in variables: # verify var nomenclature has been defined in csv, beforehand
                self.names.assert_shortname_is_defined(var)
            self.ads_variables = [self.get_ads_name(var) for var in variables] # get ads name equivalent from short name
        else:
            shortnames = [self.names.get_shortname(var) for var in variables]
            self.ads_variables = [self.get_ads_name(var) for var in shortnames] 
        
        # transform function name to extract only the acronym
        acronym = ''.join([i[0] for i  in self.model.__name__.upper().split('_')])
        # ex: global_atmospheric_composition_forecast → 'GACF'
        
        # output file path
        file_path = self.directory / Path(self._get_filename(shortnames, d, acronym, area)) # get file path
        
        if not file_path.exists():  # download if not already present
            if self.offline:        # download needed but deactivated → raise error
                raise ResourceWarning(f'Could not find local file {file_path}, offline mode is set')
                
            if self.verbose:
                print(f'downloading: {file_path.name}')
            self.model(self, file_path, d, area) # download file
            
        elif self.verbose: # elif → file already exists
            print(f'found locally: {file_path.name}')
                
        return file_path
        
            
    def get_ads_name(self, short_name):
        """
        Returns the variable's ADS name from the shortnames (ADS names are used to querry the Atmospheric Data Store)
        """
        # verify beforehand that the var has been properly defined
        if short_name not in list(self.model_specs['short_name'].values):
            raise KeyError(f'Could not find short_name \'{short_name}\' in csv file')
        
        return self.model_specs[self.model_specs['short_name'] == short_name]['ads_name'].values[0]
    
    
    def _parse_cdsapirc(self):
        """
        after retrieval the function sets attributes cdsapi_url and cdsapi_key to
        pass as parameter in the Client constructor
        """
        # taken from ECMWF's cdsapi code
        dotrc = os.environ.get("CDSAPI_RC", os.path.expanduser("~/.cdsapirc"))
        config = cdsapi_parser.read_config('ads', dotrc) 
        # save the credentials as attributes
        return config