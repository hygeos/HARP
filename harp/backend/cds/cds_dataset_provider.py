from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Collection
import uuid

import cdsapi
from core import log
from core import table
from core.save import to_netcdf
from core.static import abstract, interface
from core.fileutils import filegen
from core.save import to_netcdf
from core import auth
import xarray as xr

from harp.backend.timespec import RegularTimespec
from harp.backend.baseprovider import BaseDatasetProvider
from harp.backend.nomenclature import Nomenclature
from harp.backend import harp_std

from harp.backend import cds

@abstract
class CdsDatasetProvider(BaseDatasetProvider): 
    
    url = "https://cds.climate.copernicus.eu/api"
    keywords = ["ECMWF"]
    institution = "ECMWF"
    collection = "Undefined"
    
    name = "dataset-query-name"
    product_type = "product type"
    
    timespecs = RegularTimespec(timedelta(seconds=0), 24) # default specs to hourly from 00:00 to 23:00
    
    def __init__(self, *, config_subsection: str, csv_files: list[Path], **kwargs):
    
        super().__init__(config_subsection=config_subsection, **kwargs)
        self.internal_table = cds.cds_table(csv_files)
        self.nomenclature = Nomenclature(self.internal_table.files, cols=["harp_name", "short_name", "cds_name"], raw_col="cds_name", context=self.name)
    
    def download(self, variables: list[str], time: datetime|list[datetime, datetime], *, offline=False, area: dict=None) -> list[Path]:
        
        if area is not None:
            log.error("Regionalized query not implemented yet (area parameter)", e=ValueError)
        
        if isinstance(time, Collection): 
            log.error("Time range query not implemented yet", e=ValueError)
            
        queries = self._decompose_query(variables, time, area=area)
        
        for query in queries:
            log.debug("Querying CDS for variables: ", query["variables"], 
                      "\n -> date: ", date(query["years"], query["months"], query["days"]).strftime("%Y%m%d"),
                      "\n -> timesteps: ", query["times"])
            with TemporaryDirectory() as tmpdir:
                tmpfile = Path(tmpdir) / f"tmp_{uuid.uuid4().hex}_.nc"
                self._execute_cds_request(tmpfile, query, area=area)
                
                ds = xr.open_dataset(tmpfile, engine='netcdf4')
                
                # rename valid_time dimension to time
                # rename shortnames to cds_names for consistency
                new_names = {"valid_time": "time"} 
                for v in ds.data_vars:
                    new_names[v] = self.internal_table.get_cdsname(v)
                
                ds = ds.rename(new_names)
                    
                # split and store per variable, per timestep
                self._split_and_store_atomic(ds)
        
        files = self._get_query_files(variables, time)
        return files
    
    @abstract
    def _execute_cds_request(self, target_filepath: Path, query, area: dict=None):
        
        # TODO area
        
        dataset = self.name
        request = {
                'product_type': [self.product_type],
                'variable':     query["variables"],
                'year':         query["years"],
                'month':        query["months"],
                'day':          query["days"],
                'time':         query["times"],
                'data_format':'netcdf',
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
                
    def _decompose_query(self, variables: list[str], time: datetime|list[datetime, datetime], *, area: dict=None):
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
        missing_variables = self._find_missing_variables(variables, timesteps, area=area)
        
        if len(missing_variables) == 0:
            return [] 
        
        dates = {}
        
        for timestep in timesteps:
            day = date(timestep.year, timestep.month, timestep.day)
            if day not in dates: 
                dates[day] = []
            
            dates[day].append(timestep.strftime("%H:%M"))
        
        
        queries = []
        for d in dates: # format one query per date required
            dates[d] = list(set(dates[d]))
            
            query = dict(
                years   = d.year,
                months  = d.month,
                days    = d.day,
                times   = dates[d],
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

        
    def _standardize(self, ds):
        
        # ds = ds.rename_dims({'latitude': harp_std.lat_name, 'longitude': harp_std.lon_name}) # rename merra2 names to ECMWF convention
        # ds = ds.rename_vars({'latitude': harp_std.lat_name, 'longitude': harp_std.lon_name})
        
        ds = harp_std.center_longitude(ds, center=harp_std.longitude_center)
        
        return ds