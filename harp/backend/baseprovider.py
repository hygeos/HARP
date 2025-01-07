from datetime import date, datetime
from pathlib import Path

import xarray as xr
from core.static import interface, abstract, constraint
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
        
        # check vars to compute and vars to query
        # TODO
        
        # translate harp_name to raw_names
        if raw_query:
            raw_vars = variables
            # std_vars = cls._get_std_variables(variables)
        else:
            raw_vars = self._get_raw_variables(variables)
            # std_vars = variables
        
        # TODO: download
        files = self.download(variables=raw_vars, time=time, offline=self.config.get("offline"))
        ds = xr.open_mfdataset(files, concat_dim='time', combine='nested',)
                
        # harmonize if not disabled
        if self.config.get("harmonize"): 
            ds = self._standardize(ds)
        
        if not raw_query:
            std_name_map = {raw: self.nomenclature.get_std_name(raw) for raw in raw_vars}
            ds = ds.rename_vars(std_name_map)
        
        return ds
    
        
    def __init__(self, **kwargs):
        
        self.config_subsection = None
        if "config_subsection" in kwargs:
            self.config_subsection = kwargs["config_subsection"]
            
        self.config = harp.config.Config
        self.config = self._compute_final_config(self.config_subsection, kwargs)
    
        self._check_config()
    
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
        
        constraint.path(context="HARP config", exists=True, mode="dir").check(
            self.config.get("dir_storage")
        )

    def _get_raw_variables(self, variables) -> list[str]:
        if not hasattr(self, "nomenclature"): 
            log.error(f"DatasetProvider {self.__name__} does not contain a nomenclature attribute", e=KeyError)
        return [self.nomenclature.get_raw_name(var) for var in variables]
        
    def _get_std_variables(self, variables) -> list[str]:
        if not hasattr(self, "nomenclature"): 
            log.error(f"DatasetProvider {self.__name__} does not contain a nomenclature attribute", e=KeyError)
        return [self.nomenclature.get_std_name(var) for var in variables]
    
    @abstract # to be defined by subclasses
    def download() -> Path: # TODO add get params or smt
        raise RuntimeError('Should not be executed here, but through subclasses')

    @abstract # to be defined by subclasses
    def _standardize(ds: xr.Dataset) -> xr.Dataset:
        raise RuntimeError('Should not be executed here, but through subclasses')
    