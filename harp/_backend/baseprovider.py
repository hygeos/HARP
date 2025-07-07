from datetime import date, datetime
from pathlib import Path
from typing import Callable

import xarray as xr
from core.static import interface, abstract, constraint
from core.save import to_netcdf
from core import log
from core.config import Config

from harp._backend.nomenclature import Nomenclature
import harp.config
from copy import copy, deepcopy

from harp._backend.computable import Computable

@abstract
class BaseDatasetProvider:
    
    @interface
    def get(self,
            time: datetime, # type dictates if dt or range
            *,
            area: dict=None, 
            ) -> xr.Dataset:
        """
        Download and apply post-process to the downloaded data for the given date
        variables is a dict of the form:
            new_name: provider_specific_nane
            
            ex: {"temp": "2m_temperature"}
        Standardize the dataset:
            - names
            - longitude uses [-180; 180] convention 
            - make longitude circular (-180 AND +180 by duplicating)
        Return data interpolated on time=dt
     
        """
                
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        query       = [] # variables to query
        operands    = [] # operands for computable variables
        computed    = [] # variables to compute from operands
        
        for dst_var in self.variables:
            src_var = self.variables[dst_var]
            if isinstance(src_var, Computable):
                computed += [dst_var]
                log.debug(f"Using computable bind: {dst_var} = {src_var.func.__name__} + {src_var.operands}")
                operands += src_var.operands
            else:
                query.append(self.variables[dst_var]) # append raw var
        
        reversed_aliases = {v: k for k, v in self.variables.items() if type(v) is str} # convert aliases from std: raw to raw: std for renaming queried vars 
        operands = list(set(operands))  # get unique list 
        
        direct_query = query.copy()
        query += operands           # append operands to the list of variables to download
        query = list(set(query))
        
        for dst_var in query: # check that every raw variable exist in the dataset provider nomenclature 
            self.nomenclature.check_has_raw_name(dst_var)
            
        files = self.download(variables=query, time=time, offline=self.config.get("offline"))
        ds = xr.open_mfdataset(files, engine='netcdf4')
                
        # harmonize if not disabled
        if self.config.get("harmonize"): 
            ds = self._standardize(ds)
        
        keep = direct_query.copy()
        
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
            log.debug(f"Droping operangs: {drop}")
            ds = ds.drop_vars(drop)
        
        if reversed_aliases:
            ds = ds.rename_vars(reversed_aliases)
        
        return ds
    
    
    @interface    
    def __init__(self, variables: dict[str: str], config: dict={}):
        
        self.variables = variables
        
        # Load default config and override keys passed through the config dict parameter
        self.config = harp.config.default_config.copy()
        self.config.ingest(config, override=True)
        
        self._check_config()
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
                            if_exists="skip"
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
    
    
    @abstract # to be defined by subclasses
    def download() -> Path: # TODO add get params or smt
        raise RuntimeError('Should not be executed here, but through subclasses')

    @abstract # to be defined by subclasses
    def _standardize(ds: xr.Dataset) -> xr.Dataset:
        raise RuntimeError('Should not be executed here, but through subclasses')
    