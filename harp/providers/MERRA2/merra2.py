# standard library imports
from datetime import datetime, date
from typing import Callable
from pathlib import Path
import requests

# third party imports
from pydap.cas.urs import setup_session
import xarray as xr
import numpy as np
from core.fileutils import filegen
from core.cache import cache_json 
from core.static import interface
from core import log
from core import ftp

# sub package imports
from .merra2_models import MERRA2_Models
from ..merra2_parser import Merra2Parser
from harp.providers.baseprovider import BaseProvider
from harp.nomenclature import Nomenclature
from harp.utils import wrap, center_longitude

class MERRA2(BaseProvider):
    '''
    Ancillary data provider for MERRA-2 data from NASA's GES DISC
    https://uat.gesdisc.eosdis.nasa.gov/
    uses the OPeNDAP protocol, and credentials from .netrc file
    
    currently only supports single levels variables
    
    - model: valid MERRA2 models are listed in MERRA2.models object
             /!\\ currently only single levels are supported
    - directory: local folder path, where to download files 
    - config_file: path to the local file where to store the web-scraped MERRA2 config data
    - no_std: bypass the standardization to the nomenclature module, keeping the dataset as provided
    
    '''
    
    models = MERRA2_Models
    
    
    def standardize(self, ds):
        '''
        Modify a MERRA2 dataset variable names according to the name standard from nomenclature.py
        '''
        
        ds = self.names.rename_dataset(ds) # rename dataset according to nomenclature module
        
        ds = ds.rename_dims({'lat': 'latitude', 'lon': 'longitude'}) # rename merra2 names to ECMWF convention
        ds = ds.rename_vars({'lat': 'latitude', 'lon': 'longitude'})
        
        lons = ds.longitude.values
        
        if np.min(ds.longitude) == -180 and 175.0 <= np.max(ds.longitude) < 180 :
            ds = wrap(ds, 'longitude', -180, 180)
            
        if np.isclose(np.min(lons), -180.0) and np.max(lons) > 179.0 and not np.isclose(np.max(lons), 180.0): 
            ds = wrap(ds, 'longitude', -180, 180)
        return ds
    
    @interface
    def __init__(self, model: Callable, directory: Path, nomenclature_file=None, offline:bool=False, 
                 verbose:bool=True, no_std: bool=False, merra2_layout_file: Path=None,
                ):

        name = 'MERRA2'
        # call superclass constructor 
        BaseProvider.__init__(self, name=name, model=model, directory=directory, nomenclature_file=nomenclature_file, 
                              offline=offline, verbose=verbose, no_std=no_std)        
        
        
        self.host = 'urs.earthdata.nasa.gov' # server to download from
        self.auth = ftp.get_auth(self.host)    # credentials from netrc file
        self.base_url = 'https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/' # base url for the OPeNDAP link
        # ex: https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/M2T1NXAER.5.12.4/2015/07/MERRA2_400.tavg1_2d_aer_Nx.20150705.nc4
            
            
        self.model = model.__name__ # trick to allow autocompletion
        
        if merra2_layout_file is None: # load pre-parsed config by default
            merra2_layout_file = Path(__file__).parent / "merra2_parsed_layout.json"
        
        self.merra2_layout = Path(merra2_layout_file).resolve()
        
        # parse merra2 data on specific date (to get data layout) if required
        dat = date(2022, 12, 12) # TODO change hardcoded value ?
        parser = Merra2Parser()
        cache_file = Path(self.merra2_layout)
        self.config = cache_json(cache_file, inputs="ignore")(parser.get_model_specs)(dat)
        
        # get models version, and verify all models have the same
        v = None
        for model, cfgs in self.config.items():
            if cfgs['version'] != v:
                if v is not None: 
                    raise ValueError('Different version between different MERRA2 models, file nomenclature would be ambigous \n /!\\ THIS ERROR should not happen')
                v = cfgs['version']
        
        self.ouput_file_pattern = 'MERRA2_%s_%s_%s_%s.nc' # %model %version %vars %date 
        
        if 'user' not in self.auth: 
            raise KeyError(f'Missing key \'user\' for host {MERRA2.host} in .netrc file')
        if 'password' not in self.auth: 
            raise KeyError(f'Missing key \'password\' for host {MERRA2.host} in .netrc file')
    
        self.names = Nomenclature(provider='MERRA2', csv_file=nomenclature_file)
    
    
    @interface
    def download(self, variables: list[str], d: date, area: None|list=None) -> Path:
        """
        Download the model if necessary, returns the corresponding file Path
        
        - variables: list of strings of the MERRA2 vars to download (merra2 names) ex: ['TO3', 'TQV', 'SLP']
        - d: date of the data (not datetime)
        """
        
        cfg = self.config[self.model]
        
        if not self.no_std:
            variables = [self.names.get_shortname(var) for var in variables]
        
        for var in variables:
            if var not in cfg['variables']:
                raise KeyError(f'Could not find variable {var} in model {self.model}')
        
        acronym = self.model
        file_path = self.directory / Path(self._get_filename(variables, d, acronym, area)) # get file path
        
        
        if not file_path.exists(): 
            if self.offline:
                raise ResourceWarning(f'Could not find local file {file_path}, and online mode is off')
                
            if self.verbose:
                log.info(f'downloading: {file_path.name}')
            self._download_file(file_path, variables, d, area)
        elif self.verbose:
                log.info(f'found locally: {file_path.name}')
        
        return file_path

        
 
    def _assossiate_product(self, config, variables):
        '''
        Returns a list of tupple like: [(model_name, var_list), ..]
        list is ordered (descending) by number of needed variables contained per model
        models with 0 needed vars are removed
        '''
        
        # for every model associate the list of needed variables it contains
        res = {}
        for model in config:
            res[model] = []
        
        for var in variables:
            found = False
            for model, cfg in config.items(): 
                if var in cfg['variables']: 
                    res[model].append(var)
                    found = True
            if not found:
                raise ValueError(f'Could not find any model that contains variable \'{var}\'')
        
        # list model by number of variables contained, remove models with 0 needed variables
        r = []
        for k, v in res.items():
            if len(v) == 0: 
                continue # skip useless models
            r.append((k, v))
        
        r.sort(key = lambda item: len(item[1]), reverse=True)
        return r
    
        
    
    @filegen(1)
    def _download_file(self, target: Path, variables: list[str], d: date,  area: None|list=None):
        '''
        Download a single file, contains a day of the correcsponding MERRA-2 product's data 
        uses OPeNDAP protocol. Uses a temporary file and avoid unnecessary download 
        if it is already present, thanks to fileutil.filegen 
        
        - target: path to the target file after download
        - product: string representing the merra2 product e.g 'M2T1NXSLV'
        - d: date of the dataset
        '''
                
        if isinstance(d, datetime): # TODO change
            d = d.date()
        
        # build file OPeNDAP url
        # 'https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/MERRA2/M2T1NXAER.5.12.4/2015/07/MERRA2_400.tavg1_2d_aer_Nx.20150705.nc4'
        filename = self.config[self.model]['generic_filename'] % d.strftime('%Y%m%d')
        version = self.config[self.model]['version']
        url = self.base_url + self.model + '.' + version + d.strftime('/%Y/%m/') + filename
        
        # backup in case original file doesn't exists -> it has been reprocessed
        url_bis = url.replace('400', '401') # file is reprocessed, not original 
        
        r1 = requests.get(url+'.xml')
        if r1.status_code == 404:
            r2 = requests.get(url_bis+'.xml')
            if r2.status_code == 200:
                url = url_bis
        
        # Download file
        session = requests.Session()
        session = setup_session(self.auth['user'], self.auth['password'], check_url=url)

        store = xr.backends.PydapDataStore.open(url, session=session)
        ds = xr.open_dataset(store)
        
        ds = ds[variables] # trim dataset to only keep desired variables
        
        if area is not None:
            ds = ds.sel(lat=slice(area[2],area[0]), lon=slice(area[1],area[3]))
        
        ds.to_netcdf(target)