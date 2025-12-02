import hashlib
import copy
from datetime import date, datetime
from pathlib import Path
from typing import Callable

import xarray as xr
from core import log
from core.config import Config
from core.save import to_netcdf
from core.static import abstract
            
from harp._backend._utils import ComputeLock
from harp._backend.harp_query import HarpAtomicStorageUnit, HarpQuery

from harp._backend.nomenclature import Nomenclature
import harp.config
from harp._backend.computable import Computable


@abstract
class BaseDatasetProvider:
    
    # @interface    
    def __init__(self, variables: dict[str: str], config: dict={}):
        
        self.variables = variables
        
        # Load default config and override keys passed through the config dict parameter
        self.config = harp.config.default_config.copy()
        self.config.ingest(config, override=True)
        
        self._check_config()
        self.computables = {}    # map of computable variables
        
        # to be defined in subclass
        self.nomenclature = Nomenclature(stub=True, csv=0, context="", query_col="", harp_col="") 
    
    # # @interface
    def get(self,
            time: datetime, # type dictates if dt or range
            **kwargs,  # catch-all for additional keyword arguments
            ) -> xr.Dataset:
        """
        Download and apply post-process to the downloaded data for the given date
        variables is a dict of the form:
            new_name: provider_specific_nane
            
            ex: {"surface_temperature": "t2m"}
            
        Standardize the dataset:
            - longitude uses [-180; 180[ convention 
            
        Returns as a xarray dataset, the requested data variables with encompassing time steps
        """
        
        # TODO: remove from kwargs, use explicit parameters instead
        # KWARGS management
        # optional internal parameters, specific providers are free to overload their interface with theses
        area = kwargs.get("area")
        levels = kwargs.get("levels")
        
        koffline = kwargs.pop('offline', None)
        offline = koffline if koffline is not None else self.config.get("offline")
        
        
        query    = [] # variables to query
        operands = [] # operands for computable variables
        computed = [] # variables to compute from operands
        
        # dst_var is the user given name, bound to an exisiting query_name
        # Computable variable decomposition in operands, which are then inserted
        # construct the query array (of query_names)
        for dst_var in self.variables:
            src_var = self.variables[dst_var]
            if isinstance(src_var, Computable):
                computed += [dst_var]
                log.info(f"Using computable bind: {dst_var} = {src_var.func.__name__} + {src_var.operands}")
                operands += src_var.operands
            else:
                query.append(self.variables[dst_var]) # append raw var
        
        reversed_aliases = {v: k for k, v in self.variables.items() if type(v) is str} # convert aliases from std: raw to raw: std for renaming queried vars 
        operands = list(set(operands))  # get unique list 
        
        # translate query aliases
        # translate operands aliases
        operands    = [self.nomenclature.translate_to_query_name(op) for op in operands]
        query       = [self.nomenclature.translate_to_query_name(qu) for qu in query]
        
        direct_query = query.copy()
        query += operands           # append operands to the list of variables to download
        query = list(set(query))
        
        for dst_var in query: # check that every raw variable exist in the dataset provider nomenclature 
            self.nomenclature.assert_has_query_param(dst_var)
        
        
        hq = HarpQuery(
            variables   = query, 
            time        = time, 
            offline     = offline, 
            area        = area, 
            levels      = levels, 
        )
        files = self.download(hq)
        ds = xr.open_mfdataset(files, engine='netcdf4')
                
        ds = self._standardize(ds)
        
        # unstranslate from query request to user aliased names
        operands = [self.nomenclature.untranslate_query_name(op) for op in operands]
        query    = [self.nomenclature.untranslate_query_name(qu) for qu in query]
        keep     = [self.nomenclature.untranslate_query_name(qu) for qu in direct_query]
        
        for dst_var in computed:
            comp: Computable = self.variables[dst_var]
            ds[dst_var] = comp.func(ds)
            
            if comp.keep_operands:
                for op in comp.operands:
                    log.debug(f"Keeping operand {op}")
                    keep.append(op)
        
        keep = list(set(keep))
        drop = [op for op in operands if op not in keep]
                
        if drop:
            log.info(f"Droping operangs: {drop}")
            ds = ds.drop_vars(drop)
        
        if reversed_aliases:
            ds = ds.rename_vars(reversed_aliases)
        
        return ds
    
    
    def get_config(self):
        
        return self.config.config_dict
        
    # @interface
    def _get_meta_table(self):
        """
        Returns a dataframe containing all interfaced variables with the columns:
            harp_name, query_name, long_name, units
            
        This method is meant to be overriden when necessary 
            (ex: Copernicus internal layout doesn't have these columns, conversion needed)
        """
        
        return self.nomenclature.table
        
    # @interface
    def bind_computable(self, name: str, func: Callable, operands: list[str]):
        """Binds a virtual variable via a function and the input variables required

        Args:
            name (str): name of the outputed variable
            func (Callable): function of the form lambda xr.Dataset: xr.Dataarray None
            operands (list[str]): list of the inputs variables (raw names)
            
        Note:
            func uses ds[query_name] to get operands and return the computed dataarray which Harp will insert
        """
        
        if name in self.computables:
            log.error(f"{self.__name__}: Computable variable {name} already defined", e=KeyError)
        
        for op in operands:
            self.nomenclature.check_has_query_name(op)
        
        self.computables[name] = {"func": func, "operands": operands}
        
        return

        
    
    def _check_config(self):
        context = self.__class__.__name__
        path = self.config.get("dir_storage")
        
        if path is None:
            log.error(f"HARP_CACHE_DIR, nor DIR_ANCILLARY env variables are set. Config path (Key \'dir_storage\') not provided either in direct config for {context} object", e=RuntimeError)
        
        if type(path) == str:
            path = Path(path)
        
        if not path.is_dir():
            log.disp(log.rgb.orange, "(?) Instructions to set up Harp: www.github.com/hygeos/harp/todo") # TODO proper doc link
            log.error(f"Provided storage path {path} does not exist. Please create the folder beforehand.", e=RuntimeError)
        
        if path is None:
            log.warning("Environment variable 'HARP_CACHE_DIR' and 'DIR_ANCILLARY' not set")
            log.disp(log.rgb.orange, "(?) Instructions to set up Harp: www.github.com/hygeos/harp/todo") # TODO proper doc link
            log.error(f"Storage path (Key \'dir_storage\') not provided in config for {context} object", e=RuntimeError)
    
    
    def _split_and_store_atomic(self, ds, hq: HarpQuery):
        """
        Split and store dataset per variable and per timestep
        assumes self._get_target_file_path is implemented in subclass
        
        HarpQuery only used to essentially hash the area and levels parameters
        """
        
        for var in ds.data_vars:
            for i in range(ds[var].time.size):
                
                atomic_slice = ds[[var]].isel(time=[i], drop=False)
                timestep = datetime.fromisoformat(str(atomic_slice.time.values[0]))
                
                hast = HarpAtomicStorageUnit(variable=var, time=timestep, area=hq.area, levels=hq.levels)
                
                atomic_slice_path: Path = self._get_target_file_path(hast) 
                atomic_slice_path.parent.mkdir(exist_ok=True, parents=True)
                
                # store atomic slice
                to_netcdf(ds = atomic_slice, 
                            filename = atomic_slice_path,
                            if_exists="skip"
                )
        return
    
    
    def _exists_locally(self, hast: HarpAtomicStorageUnit) -> bool:
        filepath = self._get_target_file_path(hast)
        return filepath.is_file()
    
    
    def _get_target_file_path(self, hast: HarpAtomicStorageUnit) -> Path:
        return self._get_dataset_folder() / hast.get_subpath(self.collection)
    
    
    def _get_dataset_folder(self):
        return self.config.get("dir_storage") / self.collection / self.name
    
    
    @abstract # to be defined by subclasses
    def download() -> Path: # TODO add get params or smt
        raise RuntimeError('Should not be executed here, but through subclasses')

    @abstract # to be defined by subclasses
    def _standardize(ds: xr.Dataset) -> xr.Dataset:
        raise RuntimeError('Should not be executed here, but through subclasses')
    
    
    
    def _get_hashed_query_lock(self, hq: HarpQuery) -> ComputeLock:
        """
        Returns a ComputeLock object pointing to a unique lockfile for a provided query (nest dicts)
        query must contains unique IDs (self.collection + self.name are added for uniqueness)
        """
        
        lockfile: Path = self._get_hashed_query_lockfile_path(hq)
        
        lock = ComputeLock(
            filepath = lockfile, 
            timeout  = self.config.get("lock_timeout"),
            lifetime = self.config.get("lock_lifetime"),
            interval = 1,
        )
        
        return lock
    
        
    def _get_hashed_query_lockfile_path(self, hq: HarpQuery) -> Path:
        """
        Returns a path for a unique lockfile for a provided query (nest dicts)
        query must contains unique IDs (self.collection + self.name are added for uniqueness)
        """
        
        h = hashlib.blake2b(digest_size=16)  # 16 bytes = 128-bit digest
        h.update(str(hq).encode('utf-8'))
        h = h.hexdigest()
        
        lockfile = f"{self.collection}_{self.name}__" + h + ".lock"
        
        return self._get_query_hash_folder() / lockfile
    
        
    def _get_query_hash_folder(self) -> Path:
        """
        Returns HARP locks folder
        """
        folder = self.config.get("dir_storage") / "locks"
        
        return folder
        
    
    def _filter_cached_variables_from_queries(self, queries: list[HarpQuery]):
        """
        For each query removes the variables which are present locally (harp cache)
        Returns a list of queries
        """
                
        _queries = queries.copy()
        
        # remove emptied queries
        _queries = [self._filter_cached_variables_from_query(q) for q in _queries]
        _queries = [q for q in _queries if q != None]
        
        return _queries
        
    
    def _filter_cached_variables_from_query(self, hq: HarpQuery):
        """
        Requires a "times" section in query which is a list of datetimes
        """
        
        # translate query names to storage names (CDS query via cds_name but returns short_name)
        stored_variables = {self.nomenclature.untranslate_query_name(v): v for v in hq.variables}
        
        for v in stored_variables.keys(): # For each variable (stored != cds_name but short_name)
            all_timesteps_stored_locally = True                     
        
            for t in hq.timesteps: # Check that all timesteps are present
                
                hast = HarpAtomicStorageUnit(variable=v, time=t, area=hq.area, levels=hq.levels)
                if not self._exists_locally(hast): # already missing one, need to query anyway
                    all_timesteps_stored_locally = False
                    break
            
            if all_timesteps_stored_locally:
                log.debug(log.rgb.green, "Found locally: ", v, " for ", hq.timesteps, flush=True)
                
                _query_name = stored_variables[v]
                hq.variables.remove(_query_name)
        
        if len(hq.variables) == 0:
            hq = None
        
        return hq
    
    
    def _get_query_files(self, hq: HarpQuery):
        """
        From the query object, returns all expected atomic slices paths
        """
        
        assert type(hq.time) == datetime
        
        timesteps = self.timespecs.get_encompassing_timesteps(hq.time)
        
        times_tmp = hq.timesteps
        hq.timesteps  = timesteps
        
        files = []
        units = hq.get_atomic_storage_units()
        # for t in hq.times:
            # for var in variables:
        for u in units:
            files.append(self._get_target_file_path(u))
        
        hq.timesteps = times_tmp
        
        return files
        
    
    def _decompose_into_subqueries(self, hq: HarpQuery, **kwargs) -> list[HarpQuery]:
        """
        Decompose the query as a (series of) CDS query for the missing data,
        One query per day,
        
        e.g: 23h45 -> 00T23:00 + 01T00:00 -> should be two requests
        compiled to:
                00:00, 23:00 for 00T and 01T 
        
        """
        
        timesteps = self.timespecs.get_encompassing_timesteps(hq.time)
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
            
            hqs = HarpQuery(
                variables   = hq.variables, 
                timesteps   = timesteps, 
                area        = hq.area, 
                levels      = hq.levels, 
                offline     = hq.offline
            )
            hqs.extra["day"] = d
            
            queries.append(hqs)
        
        return queries