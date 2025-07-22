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

from harp._backend.cds import cds_search_preprocess
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
        
    
    def download(self, variables: list[str], time: datetime|list[datetime, datetime], *, offline=False, area: dict=None) -> list[Path]:
        
        if area is not None:
            log.error("Regionalized query not implemented yet (area parameter)", e=ValueError)
        
        if isinstance(time, Collection): 
            log.error("Time range query not implemented yet", e=ValueError)
            
        queries = self._decompose_query_per_day(variables, time, area=area)
        queries = self._remove_locally_present_variables_from_queries(queries, area=area)
        
        for query in queries:
            
            lockfile: Path = self._get_lockfile_from_query(query)
            
            lock = ComputeLock(
                filepath = lockfile, 
                timeout  = self.config.get("lock_timeout"),
                lifetime = self.config.get("lock_lifetime"),
                interval = 1,
            )
            
            
            if lock.is_locked():
                lock.wait()
                
                query = self._remove_locally_present_variables_from_query(query)
                if query == None: continue # all files present locally
                
            with lock.locked(): # lock query
            
                # log.debug("QUERY:\n", query)
                log.info(f"Querying {self.name} for variables {', '.join(query['variables'])} on {query['years']}-{query['months']}-{query['days']} {query['times']}")
                        
                with TemporaryDirectory() as tmpdir:
                    tmpfile = Path(tmpdir) / f"tmp_{uuid.uuid4().hex}_.nc"
                    self._execute_cds_request(tmpfile, query, area=area)
                    
                    ds = xr.open_dataset(tmpfile, engine='netcdf4')
                    
                    # rename valid_time dimension to time
                    # rename shortnames to query_names for consistency
                    new_names = {"valid_time": "time"} 
                    
                    ds = ds.rename(new_names)
                        
                    # split and store per variable, per timestep
                    self._split_and_store_atomic(ds)
                    
        
        returned_params = [self.nomenclature.untranslate_query_name(p) for p in variables]
        
        files = self._get_query_files(returned_params, time)
        return files
    
    
            
    
    def _get_lockfile_from_query(self, query) -> Path:
        """
        Returns a path for a unique lockfile
        TODO: consider moving to BaseProvider
        """
        
        _query = copy.deepcopy(query)
        del _query["_timesteps"]
        
        h = hashlib.blake2b(digest_size=16)  # 16 bytes = 128-bit digest
        h.update(str(_query).encode('utf-8'))
        h = h.hexdigest()
        
        lockfile = f"{self.collection}_{self.name}__" + h + ".lock"
        lockfile = self.config.get("dir_storage") / "locks" / lockfile
        
        return lockfile
    
    
    @abstract
    def _execute_cds_request(self, target_filepath: Path, query, area: dict=None):
        
        # TODO area
        
        dataset = self.name
        request = {
                "product_type":     [self.product_type],
                "variable":         query["variables"],
                "year":             query["years"],
                "month":            query["months"],
                "day":              query["days"],
                "time":             query["times"],
                "data_format":      "netcdf",
                "download_format":  "unarchived"
        }
        
        # if area is not None: 
            # request['area'] = area
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
        
    
    def _get_query_files(self, variables: list[str], time: datetime|list[datetime, datetime], *, area: dict=None):
        timesteps = self.timespecs.get_encompassing_timesteps(time)
        
        files = []
        
        for timestep in timesteps:
            for var in variables:
                files.append(self._get_target_file_path(var, timestep))
                
        return files
        
                
    def _decompose_query_per_day(self, variables: list[str], time: datetime|list[datetime, datetime], *, area: dict=None):
        """
        Decompose the query as a (series of) CDS query for the missing data,
        One query per day,
        Can choose to download more to avoid doing several queries
        
        e.g: 23h45 -> 00T23:00 + 01T00:00 -> should be two requests
        compiled to:
                00:00, 23:00 for 00T and 01T 
        
        """
        
        if isinstance(time, Collection): # TODO: should be easy to add with current functionning
            log.error("Time range query not implemented yet", e=ValueError)
        
        timesteps = self.timespecs.get_encompassing_timesteps(time)
        
        # NOTE: moving logic out of decomposition routine
        # missing_variables = self._find_missing_variables(variables, timesteps, area=area)
        
        # if len(missing_variables) == 0:
        #     return [] 
        
        dates = {}
        
        for timestep in timesteps:
            day = date(timestep.year, timestep.month, timestep.day)
            if day not in dates: 
                dates[day] = []
            
            dates[day].append(timestep)
        
        
        queries = []
        for d in dates: # format one query per date required
            dates[d] = list(set(dates[d]))
            
            query = dict(
                years   = d.year,
                months  = d.month,
                days    = d.day,
                times   = [t.strftime("%H:%M") for t in dates[d]],
                _timesteps = dates[d],
                variables = variables.copy(),
            )
            queries.append(query)
        
        return queries
        
    
    def _remove_locally_present_variables_from_queries(self, queries, area=None):
        """
        Modifies inplace queries to remove variables which are present locally (harp cache)
        """
        
        _queries = copy.deepcopy(queries)
        
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
            
        # remove emptied queries
        _queries = [self._remove_locally_present_variables_from_query(q) for q in _queries]
        _queries = [q for q in _queries if q != None]
        
        return _queries
        
        
    def _remove_locally_present_variables_from_query(self, query, area=None):
        
        _query = copy.deepcopy(query)
        
        stored_variables = {self.nomenclature.untranslate_query_name(v): v for v in _query["variables"]}
        
        for variable in stored_variables.keys():                    # For each variable (stored != cds_name but short_name)
            all_timesteps_stored_locally = True                     
        
            for timestep in _query["_timesteps"]:                    # Check that all timesteps are present
                if not self._exists_locally(variable, timestep):    # already missing one, need to query anyway
                    all_timesteps_stored_locally = False
                    break
            
            if all_timesteps_stored_locally:
                log.debug(log.rgb.green, "Found locally: ", variable, " for ", _query["_timesteps"], flush=True)
                
                _query_name = stored_variables[variable]
                _query["variables"].remove(_query_name)
        
        if len(_query["variables"]) == 0:
            _query = None
        
        return _query

        
    def _standardize(self, ds):
        
        # ds = ds.rename_dims({'latitude': harp_std.lat_name, 'longitude': harp_std.lon_name}) # rename merra2 names to ECMWF convention
        # ds = ds.rename_vars({'latitude': harp_std.lat_name, 'longitude': harp_std.lon_name})
        
        ds = harp_std.center_longitude(ds, center=harp_std.longitude_center)
        
        return ds
        
        
    # plug the format search table function
    format_search_table = cds_search_preprocess.format_search_table
    

