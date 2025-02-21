from datetime import date, datetime
from pathlib import Path
import json
from typing import Collection
import warnings

from pydap.cas.urs import setup_session
import xarray as xr
from core import log
from core.static import interface
from core.fileutils import filegen
from core.save import to_netcdf
from core import auth

import requests

from harp.providers.NASA.MERRA2 import _layout

from harp.backend.baseprovider import BaseDatasetProvider
from harp.backend.nomenclature import Nomenclature
from harp.backend import harp_std

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
        
        self.nomenclature = Nomenclature(self.variables_csv_path, cols=["raw_name"], raw_col="raw_name", context="MERRA2")

    @interface
    def download(self, variables: list[str], time: datetime, *, area: dict=None, offline=False) -> list[Path]:
        """
        variables are expected to be raw
        """
        
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        queries = self._decompose_query(variables, time, area=area)
        
        for query in queries:
            if offline:
                log.error(f"Offline mode is activated and data is missing locally [{', '.join(query)}] for {time.strftime('%Y-%m-%d')}",
                    e=FileNotFoundError)
                    
            log.debug(f"Querying {self.name} for variables {', '.join(query['variables'])} on {query['date']} {query['times']}")

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
            
            missing_variables = self._find_missing_variables(variables, compiled_times, area=area)
            
            query = dict(
                date      = d,
                times     = compiled_times,
                variables = missing_variables,
            )
            queries.append(query)
            
        return queries
        
    
    def _find_missing_variables(self, variables, timesteps, area=None):
        
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        missing_variables = []
        
        for variable in variables:
            data_fully_present_locally = True
            
            for timestep in timesteps:
                if not self._exists_locally(variable, timestep):
                    data_fully_present_locally = False
                    break 
                
            if not data_fully_present_locally: 
                missing_variables.append(variable)
        
        return missing_variables
    
    
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