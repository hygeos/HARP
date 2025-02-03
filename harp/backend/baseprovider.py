from datetime import date, datetime
from pathlib import Path
from typing import Callable

import xarray as xr
from core.static import interface, abstract, constraint
from core.save import to_netcdf
from core import log
from core.config import Config

from harp.backend.nomenclature import Nomenclature
import harp.config
from copy import copy, deepcopy

@abstract
class BaseDatasetProvider:
    
    @interface
    def get(self,
            variables: list[str], 
            time: datetime, # type dictates if dt or range
            *,
            area: dict=None, 
            raw_query = False,
            ) -> xr.Dataset:
        """
        Download and apply post-process to the downloaded data for the given date
        Standardize the dataset:
            - names
            - longitude uses [-180; 180] convention 
            - make longitude circular (-180 AND +180 by duplicating)
        Return data interpolated on time=dt
     
        """
                
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        # Computables variables management
        operands = []
        computed = []
        
        for var in variables:
            if var in self.computables:
                variables.remove(var)
                c = self.computables[var]
                log.debug(f"Using computable bind: {var} = {c['func'].__name__} + {c['operands']}")
                computed += [var]
                operands += c["operands"]
        
        operands = list(set(operands)) # get unique list 
        
        # Translation to raw names 
        raw_vars = variables
        if not raw_query:
            raw_vars = list(self._get_var_map_raw_to_std(variables).keys())
        
        for var in raw_vars:
            self.nomenclature.check_has_raw_name(var)
            
        # append computable operands
        raw_vars += operands 
        
        files = self.download(variables=raw_vars, time=time, offline=self.config.get("offline"))
        ds = xr.open_mfdataset(files, engine='netcdf4')
                
        # harmonize if not disabled
        if self.config.get("harmonize"): 
            ds = self._standardize(ds)
        
        for var in computed:
            fn = self.computables[var]["func"]
            ds[var] = fn(ds)
        
        if not raw_query:
            std_name_map = self._get_var_map_raw_to_std(variables)
            ds = ds.rename_vars(std_name_map)
        
        return ds
    
        
    def __init__(self, **kwargs):
        
        self.config_subsection = None
        if "config_subsection" in kwargs:
            self.config_subsection = kwargs["config_subsection"]
            
        self.config = harp.config.Config
        self.config = self._compute_final_config(self.config_subsection, kwargs)
        
        self._check_config()
        self.binds = {}         # map of runtime defined std_names -> raw_names
        self.computables = {}    # map of computable variables
        
    @interface
    def _get_meta_table(self):
        """
        Returns a dataframe containing all interfaced variables with the columns:
            harp_name, raw_name, long_name, units
            
        This method is meant to be overriden when necessary 
            (ex: Copernicus internal layout doesn't have these columns, conversion needed)
        """
        
        return self.nomenclature.table
        
    @interface
    def bind_computable(self, name: str, func: Callable, operands: list[str]):
        """Binds a virtual variable via a function and the input variables required

        Args:
            name (str): name of the outputed variable
            func (Callable): function of the form lambda xr.Dataset: xr.Dataarray None
            operands (list[str]): list of the inputs variables (raw names)
            
        Note:
            func uses ds[raw_name] to get operands and return the computed dataarray which Harp will insert
        """
        
        if name in self.computables:
            log.error(f"{self.__name__}: Computable variable {name} already defined", e=KeyError)
        
        for op in operands:
            self.nomenclature.check_has_raw_name(op)
        
        self.computables[name] = {"func": func, "operands": operands}
        
        return
        
    @interface
    def bind(self, variables: dict):
        
        self.nomenclature: Nomenclature
        
        for std, raw in variables.items():
            
            assert type(std) == str
            assert type(raw) == str
            
            self.nomenclature.check_has_raw_name(raw)
            self.binds[std] = raw
    
    @interface
    def _compute_final_config(self, subsection: str|None, explicitly_passed_config: dict):
        """
        Steps:
        1: Loads the Harp defaults config
        2: Overrides the keys presents in the provided subsection (if exists) (from toml)
        3: Overrides the keys directly provided by constructor 
        """
        
        config_obj = harp.config.general_config.copy() # init configuration with harp default general config
        
        if subsection: # if provided ingest subsection as override
            config_obj.ingest(
                harp.config.harp_config.get_subsection(subsection, default={}),
                override=True
            )
        
        config_obj.ingest(explicitly_passed_config, override=True)
        return config_obj
        
    
    def _check_config(self):
        context = self.__class__.__name__
        path = self.config.get("dir_storage")
        if path is None:
            log.error(f"Key \'dir_storage\' not provided in config for {context} object", e=RuntimeError)
            
        c = constraint.path(context=f"{context} object config", exists=True, mode="dir")
        c.check(self.config.get("dir_storage"))
        # log.warning(log.rgb.red, self.config.get("dir_storage"))
    
    
    def _split_and_store_atomic(self, ds):
        """
        Split and store dataset per variable and per timestep
        assumes self._get_target_file_path is implemented in subclass
        """
    
        for var in ds.data_vars:
            for i in range(ds[var].time.size):
                
                atomic_slice = ds[[var]].isel(time=[i], drop=False)
                timestep = datetime.fromisoformat(str(atomic_slice.time.values[0]))
                atomic_slice_path: Path = self._get_target_file_path(var, timestep) 
                atomic_slice_path.parent.mkdir(exist_ok=True, parents=True)
                
                # store atomic slice
                to_netcdf(ds = atomic_slice, 
                            filename = atomic_slice_path,
                            if_exists="error"
                )
        return
    
    def _exists_locally(self, variable: str, time: datetime) -> bool:
        filepath = self._get_target_file_path(variable, time)
        return filepath.is_file()
    
    
    def _get_target_file_path(self, variable: str, time: datetime) -> Path:
        return self.config.get("dir_storage") / self._get_target_subfolder(time) / self._get_target_filename(variable, time)
    
    
    def _get_target_subfolder(self, time: datetime):
        return Path() / self.institution / self.collection / self.name / time.strftime("%Y/%m/%d")
    
    
    def _get_target_filename(self, variable, time: datetime, region=False):
        
        filestr = f"{self.collection}_{self.name}_"
        filestr += "region_" if region else "global_"
        filestr += time.strftime("%Y-%m-%dT%H:%MZ_")
        filestr += f"_{variable}_"
        filestr += f"{self._get_storage_version()}.nc"
        
        return filestr    
    
    
    def _get_storage_version(self):
        """
        Allow to invalidate cached Harp dataset by incrementing this version number
        Shoud increment only if needed, doesn't need to be phased with package version
        """
        
        version = "2.0.2"
        
        return "hsv" + version
    
    
    def _get_var_map_raw_to_std(self, std_variables) -> dict[str]:
        """
        Returns a dict of raw_name: std_name, includes the variables binded at runtime 
        """
        vmap = self._get_var_map_std_to_raw(std_variables)
        
        vmap = {v: k for k, v in vmap.items()}
        return vmap
    
    def _get_var_map_std_to_raw(self, std_variables) -> dict[str]:
        """
        Returns a dict of std_name: raw_name, includes the variables binded at runtime 
        """
        
        if not hasattr(self, "nomenclature"): 
            log.error(f"DatasetProvider {self.__name__} does not contain a nomenclature attribute", e=KeyError)
        
        vmap = {}
        
        for var in std_variables:
            if var in self.binds:
                raw = self.binds[var]
                # log.debug(f"Using runtime variable binding {var} -> {raw}")
            else:
                raw = self.nomenclature.get_raw_name(var)
            vmap[var] = raw
        
        return vmap
    
    @abstract # to be defined by subclasses
    def download() -> Path: # TODO add get params or smt
        raise RuntimeError('Should not be executed here, but through subclasses')

    @abstract # to be defined by subclasses
    def _standardize(ds: xr.Dataset) -> xr.Dataset:
        raise RuntimeError('Should not be executed here, but through subclasses')
    