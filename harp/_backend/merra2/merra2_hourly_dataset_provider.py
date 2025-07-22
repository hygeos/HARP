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
from harp._backend.baseprovider import BaseDatasetProvider
from harp._backend.merra2 import merra2_search_preprocess
from harp._backend.nomenclature import Nomenclature
from harp.providers.NASA.MERRA2 import _layout

warnings.filterwarnings('ignore', message='PyDAP was unable to determine the DAP protocol*')


class Merra2HourlyDatasetProvider(BaseDatasetProvider):
    
    keywords = ["NASA", "GMAO"]
    institution = "NASA"
    
    host = 'urs.earthdata.nasa.gov' # server to download from
    auth = auth.get_auth(host)  # credentials from netrc file
    
    
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
    def download(self, variables: list[str], time: datetime, *, area: dict=None, offline=False) -> list[Path]:
        """
        variables are expected to be raw
        """
        
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        queries = self._decompose_query(variables, time, area=area)
        queries = self._filter_cached_variables_from_queries(queries, area=area)
        
        for query in queries:
            
            lock: ComputeLock = self._get_hashed_query_lock(query)
            
            if lock.is_locked(): # query is currently already being executed by someone in the same HARP CACHE DIR tree
                lock.wait()
            
                query = self._filter_cached_variables_from_query(query) # Check to see if all necessary files are now present
                if query == None: continue # all files present locally
            
            with lock.locked(): # lock query and make query download
                if offline:
                    log.error(f"Offline mode is activated and data is missing locally [\
                        {', '.join(query['variables'])}] for {time.strftime('%Y-%m-%d')}",
                        e=FileNotFoundError)
                
                log.info(f"Querying {self.name} for variables {', '.join(query['variables'])} on {query['date']} {query['times']}")

                ds = self._access_day_file(query["date"], area)
                ds = ds[query["variables"]].sel(time=query["times"]).compute()
                
                # split and store per variable, per timestep
                self._split_and_store_atomic(ds)
            
        files = self._get_query_files(variables, time)
        return files
    
    
    def _get_query_files(self, variables: list[str], time: datetime|list[datetime, datetime], *, area: dict=None):
        timesteps = self.timespecs.get_encompassing_timesteps(time)
        
        files = []
        
        for timestep in timesteps:
            for var in variables:
                files.append(self._get_target_file_path(var, timestep))
                
        return files
    
    def _access_day_file(self, day: date,  area: dict=None) -> xr.Dataset:
        """
        Download to a temporary dir the whole day, returns the dataset trimmed with OPeNDAP 
        """
        
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
    
    def _decompose_query(self, variables: list[str], time: datetime|list[datetime, datetime], *, area: dict=None):
        """
        Decompose the query as a (series of) Pydap query for the missing data,
        Can choose to download more to avoid doing several queries
        
        e.g: 23h45 -> 00T23:00 + 01T00:00 -> should be two requests
        compiled to:
                00:00, 23:00 for 00T and 01T 
        
        """
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        if isinstance(time, Collection): # TODO: should be easy to add with current functionning
            log.error("Time range query not implemented yet", e=ValueError)
        
        timesteps = self.timespecs.get_encompassing_timesteps(time)
        dates = {}
        
        for timestep in timesteps:
            day = date(timestep.year, timestep.month, timestep.day)
            if day not in dates: 
                dates[day] = []
            
            dates[day].append(timestep)
        
        
        queries = []
        for d in dates: # format one query per date required
            dates[d] = list(set(dates[d]))
            compiled_times = dates[d]
            
            query = dict(
                date      = d,
                times     = compiled_times,
                variables = variables,
            )
            queries.append(query)
            
        return queries
    
    
    
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
    format_search_table = merra2_search_preprocess.format_search_table