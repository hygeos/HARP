from datetime import datetime
from pathlib import Path
from core import log
import hashlib


class HarpAtomicStorageUnit:
    
    storage_version = "v03"
    
    def __init__(self, *,
        variable: str, 
        time: datetime,
        area: dict = None,
        levels: list[int] = None,
        ref_time: datetime = None,
    ):
    
        self.variable = variable
        self.time     = time
        self.area     = area
        self.levels   = None if levels is None else sorted(levels)
        self.ref_time = None if ref_time is None else ref_time
        
        
    def get_subpath(self, prefix: str) -> Path:
        """
        Returns the atomic slice sub path (subtree from the Dataset)
        """
        
        if prefix == "":
            log.error("Missing prefix")
        
        hs = ""
        if self.levels or self.area or self.ref_time:
            hs = "area:" + str(self.area) + "; levels:" + str(self.levels)
            
            if self.ref_time: hs += "; ref:" + str(self.ref_time) # add ref time management without breaking already stored filenames
            
            h = hashlib.blake2b(digest_size=24)  # 24 bytes = 192-bit digest, less probable collision than 128-bit (collision virtually impossible)
            h.update(str(hs).encode('utf-8'))
            hs = h.hexdigest() + "_"
        
        
        filestr = prefix
        filestr += f"_{self.variable}_"
        rg = "global_" if not self.area else log.error("Not implemented yet")
        filestr += rg
        lvl = "sl_" if not self.levels else "ml_"
        filestr += lvl
        filestr += self.time.strftime("%Y-%m-%dT%H:%MZ_")
        filestr += hs
        filestr += f"{self.storage_version}.nc"
        
        sfile = Path(filestr)
        ssubdir = Path(self.time.strftime("%Y/%m/%d")) 
        
        return ssubdir / sfile
        

class HarpQuery:
    
    def __init__(self, *,
        variables: list[str], 
        time: datetime=None, # type dictates if dt or range
        timesteps: list[datetime]=None,
        offline: bool = False,
        area: dict = None,
        levels: list[int] = None,
        ref_time: datetime = None,
    ):
        """
        Class to store a query to a dataset provider
        Args:
            variables (list[str]): list of variable names to query
            time (datetime): single datetime of query
            timesteps (list[datetime] | datetime): list of the timesteps to encompassing the query
            offline (bool, optional): if True, do not attempt to download missing data. Defaults to False.
            area (dict, optional): NOT IMPLEMENTED YET.
            levels (list[int], optional): list of pressure levels to query. Defaults to None.
            ref_time (datetime, optional): reference time for forecast datasets.
        """
    
        self.variables  = variables.copy()
        self.time       = time
        self.timesteps  = timesteps
        self.offline    = offline
        self.area       = area
        self.levels     = None if levels is None else sorted(levels)
        self.ref_time   = ref_time 
        
        # assert type(self.time) == datetime
        if self.time is not None:
            assert type(self.time) == datetime
            
        if self.timesteps is not None:
            assert type(self.timesteps) == list
        
        if isinstance(timesteps, list): self.timesteps = timesteps.copy()
        elif isinstance(timesteps, datetime): self.timesteps = [timesteps]
        
        self.extra = {} # extra data (mostly formating)

    def __dict__(self):
        
        return dict(
            variables   = self.variables,
            times       = self.timesteps,
            offline     = self.offline,
            area        = self.area,
            levels      = self.levels,
            ref_time    = self.ref_time,
        )
    
    def get_atomic_storage_units(self) -> list[HarpAtomicStorageUnit]:
        """
        Return the decomposition of the query on atomic slice storage units
        """
        
        units = []
        for v in self.variables:
            for t in self.timesteps:
                hast = HarpAtomicStorageUnit(variable=v, time=t, area=self.area, levels=self.levels, ref_time=self.ref_time)
                units += [hast]
                
        return units
        
    def __str__(self) -> str:
        
        s  = "QUERY{"
        s += "variables: " + str(self.variables) + "; "
        s += "timesteps: " + str(self.timesteps) + "; "
        s += "area: " + str(self.area) + "; "
        s += "levels: " + str(self.levels) + "; "
        s += "ref_time: " +  str(self.ref_time) + "; "
        s += "}END"
        
        return s