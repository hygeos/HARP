from datetime import date, datetime
from pathlib import Path

import xarray as xr
from core.static import interface, abstract
from core import log

from harp.utils.nomenclature import Nomenclature

@abstract
class HarpDatasetProvider:
    
    @classmethod
    @interface
    def get(cls,
            variables: list[str], 
            time: date|tuple[datetime,datetime], # type dictates if dt or range
            *,
            area: dict=None, 
            raw_query = False,
            harmonize: bool=True,
            offline=False,
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
            std_vars = cls._get_std_variables(variables)
        else:
            raw_vars = cls._get_raw_variables(variables)
            std_vars = variables
        
        # TODO: check for computed vars if not self.no_std
        # vars_to_compute, vars_to_query = ...
        
        # TODO: download
        files = cls.download(variables=raw_vars, day=time, offline=offline)
        ds = xr.open_mfdataset(files)
                
        # harmonize if not disabled
        if harmonize: 
            ds = cls._standardize(ds)
        
        if not raw_query:
            std_name_map = {raw: cls.nomenclature.get_std_name(raw) for raw in raw_vars}
            ds = ds.rename_vars(std_name_map)
        
        return ds

    @classmethod 
    def _get_raw_variables(cls, variables) -> list[str]:
        if not hasattr(cls, "nomenclature"): 
            log.error(f"DatasetProvider {cls.__name__} does not contain a nomenclature attribute", e=KeyError)
        return [cls.nomenclature.get_raw_name(var) for var in variables]
        
    @classmethod 
    def _get_std_variables(cls, variables) -> list[str]:
        if not hasattr(cls, "nomenclature"): 
            log.error(f"DatasetProvider {cls.__name__} does not contain a nomenclature attribute", e=KeyError)
        return [cls.nomenclature.get_std_name(var) for var in variables]
    
    @abstract # to be defined by subclasses
    @classmethod
    def download() -> Path: # TODO add get params or smt
        raise RuntimeError('Should not be executed here, but through subclasses')

    @abstract # to be defined by subclasses
    @classmethod
    def _standardize(ds: xr.Dataset) -> xr.Dataset:
        raise RuntimeError('Should not be executed here, but through subclasses')
    