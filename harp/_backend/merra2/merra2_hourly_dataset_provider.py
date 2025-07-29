import copy
import json
import warnings
from datetime import date, datetime
from pathlib import Path
from typing import Collection

import requests
import xarray as xr
from core import auth, log
from core.fileutils import filegen
from core.save import to_netcdf
from core.static import interface
from pydap.cas.urs import setup_session

from harp._backend import harp_std
from harp._backend._utils import ComputeLock
from harp._backend._utils.harp_query import HarpQuery
from harp._backend.baseprovider import BaseDatasetProvider
from harp._backend.merra2 import merra2_search_provider
from harp._backend.nomenclature import Nomenclature
from harp.providers.NASA.MERRA2 import _layout

warnings.filterwarnings('ignore', message='PyDAP was unable to determine the DAP protocol*')


class Merra2HourlyDatasetProvider(BaseDatasetProvider):
    
    keywords = ["NASA", "GMAO"]
    institution = "NASA"
    
    host = 'urs.earthdata.nasa.gov' # server to download from
    
    
    def __init__(self, collection: str, name: str, variables: dict[str: str], config: dict={}):
        
        layout_folder = Path(_layout.__file__).parent
        
        self.name = name
        self.collection = collection
        
        super().__init__(variables=variables, config=config)
        
        self.infos_json_path    = layout_folder / self.collection / "infos"     / f"{self.name}.json"
        self.variables_csv_path = layout_folder / self.collection / "variables" / f"{self.name}.csv"
        
        with open(self.infos_json_path, "r") as f: 
            self.infos = json.load(f)
        
        self.nomenclature = Nomenclature(self.variables_csv_path, context="MERRA2", query_col="query_name")
        self.timerange_str = "1980 … -45days"
        

    # @interface
    def download(self, hq: HarpQuery) -> list[Path]:
        """
        variables are expected to be raw
        """
        
        if hq.area is not None:
            log.error("Regionalized query not implemented yet (area parameter)", e=ValueError)
            
        subqueries: list[HarpQuery] = self._decompose_into_subqueries_per_day(hq)
        subqueries: list[HarpQuery] = self._filter_cached_variables_from_queries(subqueries)
        
        for hqs in subqueries:

            lock: ComputeLock = self._get_hashed_query_lock(hqs)
            
            if lock.is_locked(): # query is currently already being executed by someone in the same HARP CACHE DIR tree
                lock.wait()
            
                hqs = self._filter_cached_variables_from_query(hqs) # Check to see if all necessary files are now present
                if hqs == None: continue # all files present locally
            
            with lock.locked(): # lock query and make query download
                if hqs.offline:
                    log.error(f"Offline mode is activated and data is missing locally [\
                        {', '.join(hqs.variables)}] for {hq.times}",
                        e=FileNotFoundError)
                
                self.auth = auth.get_auth(self.host)  # credentials from netrc file
                
                log.info(f"Querying {self.name} for variables {', '.join(hqs.variables)} on {hqs.times}")

                ds = self._access_day_file(hqs)
                ds = ds[hqs.variables].sel(time=hqs.times).compute()
                
                # TODO area selection
                
                # split and store per variable, per timestep
                self._split_and_store_atomic(ds, hqs)
            
        files = self._get_query_files(hq)
        return files
    
    
    
    def _access_day_file(self, hq: HarpQuery) -> xr.Dataset:
        """
        Download to a temporary dir the whole day, returns the dataset trimmed with OPeNDAP 
        """
        
        day = hq.extra["day"]
        
        url = self._get_url(day)
    
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
        
        return ds
    
    
    def _get_url(self, day: date):
        
        base = Path("goldsmr4.gesdisc.eosdis.nasa.gov/opendap/")
        
        url = base / self.collection / Path(str(self.__class__.__name__) + ".5.12.4")
        subdirstr = day.strftime("%Y/%m") if self.collection == "MERRA2" else day.strftime("%Y")
        url = url / subdirstr
        filename = self.infos["generic_name"]
        url = url / day.strftime(filename)
        
        return "https://" + str(url)
        
    
    def _standardize(self, ds):
        
        ds = ds.rename_dims({'lat': harp_std.lat_name, 'lon': harp_std.lon_name}) # rename merra2 names to ECMWF convention
        ds = ds.rename_vars({'lat': harp_std.lat_name, 'lon': harp_std.lon_name})
        
        ds = harp_std.center_longitude(ds, center=harp_std.longitude_center)
        
        return ds
    
    # plug the format search table function
    format_search_table = merra2_search_provider.format_search_table