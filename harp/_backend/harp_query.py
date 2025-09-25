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
        times: datetime | list[datetime, datetime],
        offline: bool = False,
        area: dict = None,
        levels: list[int] = None,
        ref_time: datetime = None,
    ):
    
        self.variables  = variables.copy()
        self.times      = times
        self.offline    = offline
        self.area       = area
        self.levels     = None if levels is None else sorted(levels)
        self.ref_time   = ref_time 
        
        if isinstance(times, list): self.times = times.copy()
        elif isinstance(times, datetime): self.times = [times]
        
        self.extra = {} # extra data (mostly formating)

    def __dict__(self):
        
        return dict(
            variables   = self.variables,
            times       = self.times,
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
            for t in self.times:
                hast = HarpAtomicStorageUnit(variable=v, time=t, area=self.area, levels=self.levels, ref_time=self.ref_time)
                units += [hast]
                
        return units
        
    def __str__(self) -> str:
        
        s  = "QUERY{"
        s += "variables: " + str(self.variables) + "; "
        s += "timesteps: " + str(self.times) + "; "
        s += "area: " + str(self.area) + "; "
        s += "levels: " + str(self.levels) + "; "
        s += "ref_time: " +  str(self.ref_time) + "; "
        s += "}END"
        
        return s