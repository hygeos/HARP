import copy
from datetime import date, datetime, timedelta
import hashlib
from pathlib import Path
import pprint
from tempfile import TemporaryDirectory
import time
from typing import Collection
import uuid

import cdsapi
from core import log
from core.static import abstract, interface
import xarray as xr

from harp._backend._utils.harp_query import HarpQuery
from harp._backend.cds import cds_search_provider
from harp._backend.cds.cds_query import CdsAtomicQuery
from harp._backend.timespec import RegularTimespec
from harp._backend.baseprovider import BaseDatasetProvider
from harp._backend.nomenclature import Nomenclature
from harp._backend import harp_std

from harp._backend import cds
from harp._backend._utils import ComputeLock

@abstract
class CdsDatasetProvider(BaseDatasetProvider): 
    
    url = "https://cds.climate.copernicus.eu/api"
    keywords = ["ECMWF"]
    institution = "ECMWF"
    collection = "Undefined"
    
    name = "dataset-query-name"
    product_type = "product type"
    
    timespecs = RegularTimespec(timedelta(seconds=0), 24) # default specs to hourly from 00:00 to 23:00
    
    
    # @interface
    def __init__(self, *, csv_files: list[Path], variables: dict[str: str], config: dict={}):
    
        super().__init__(variables=variables, config=config)
        table = cds.cds_table(csv_files).table
        self.nomenclature = Nomenclature(
            table, 
            context=self.name, 
            query_col="query_name", 
            harp_col="short_name", 
        )
        
    
    def download(self, hq: HarpQuery) -> list[Path]:
        
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
                        {', '.join(hqs['variables'])}] for {time.strftime('%Y-%m-%d')}",
                        e=FileNotFoundError)
            
                # log.debug("QUERY:\n", query)
                d = hqs.extra["day"]
                log.info(f"Querying {self.name} for variables {', '.join(hqs.variables)} on {d.year}-{d.month}-{d.day} {hqs.times}")
                        
                with TemporaryDirectory() as tmpdir:
                    tmpfile = Path(tmpdir) / f"tmp_{uuid.uuid4().hex}_.nc"
                    
                    self._execute_cds_request(tmpfile, hqs)
                    
                    ds = xr.open_dataset(tmpfile, engine='netcdf4')
                    
                    # rename valid_time dimension to time
                    # rename shortnames to query_names for consistency
                    new_names = {"valid_time": "time"} 
                    
                    ds = ds.rename(new_names)
                        
                    # split and store per variable, per timestep
                    self._split_and_store_atomic(ds, hqs)
                    
        
        returned_params = [self.nomenclature.untranslate_query_name(p) for p in hq.variables]
        hq.variables = returned_params
        
        files = self._get_query_files(hq)
        return files
    
    
    @abstract
    def _execute_cds_request(self, target_filepath: Path, hq: HarpQuery, ):
        return
        
    
    def _get_query_files(self, hq: HarpQuery):
        timesteps = self.timespecs.get_encompassing_timesteps(hq.times)
        # TODO manage multiple times
        
        times_tmp = hq.times
        hq.times  = timesteps
        
        files = []
        units = hq.get_atomic_storage_units()
        # for t in hq.times:
            # for var in variables:
        for u in units:
            files.append(self._get_target_file_path(u))
        
        hq.times = times_tmp
        
        return files
        
                
    def _decompose_into_subqueries_per_day(self, hq: HarpQuery) -> list[HarpQuery]:
        """
        Decompose the query as a (series of) CDS query for the missing data,
        One query per day,
        Can choose to download more to avoid doing several queries
        
        e.g: 23h45 -> 00T23:00 + 01T00:00 -> should be two requests
        compiled to:
                00:00, 23:00 for 00T and 01T 
        
        """
        
        if len(hq.times) > 1: # TODO: should be easy to add with current functionning
            log.error("Time range query not implemented yet", e=ValueError)
        
        timesteps = self.timespecs.get_encompassing_timesteps(hq.times) # TODO
        
        dates = {}
        
        for timestep in timesteps:
            day = date(timestep.year, timestep.month, timestep.day)
            if day not in dates: 
                dates[day] = []
            
            dates[day].append(timestep)
        
        
        queries = []
        for d in dates: # format one query per date required
            dates[d] = list(set(dates[d]))
            timesteps = dates[d]
            
            hqs = HarpQuery(variables=hq.variables, times=timesteps, area=hq.area, levels=hq.levels)
            hqs.extra["CDS"] = True
            hqs.extra["day"] = d
            
            queries.append(hqs)
        
        return queries
        
    def _standardize(self, ds):
        
        # ds = ds.rename_dims({'latitude': harp_std.lat_name, 'longitude': harp_std.lon_name}) # rename merra2 names to ECMWF convention
        # ds = ds.rename_vars({'latitude': harp_std.lat_name, 'longitude': harp_std.lon_name})
        
        ds = harp_std.center_longitude(ds, center=harp_std.longitude_center)
        
        return ds
        
        
    # plug the format search table function
    format_search_table = cds_search_provider.format_search_table
    

