from pathlib import Path

import xarray as xr

from core import log, rgb

def read_ifs():
    
    folder = Path("/mnt/ceph/user/joackim/data/ifs_data_example")
    
    files = [
        "aifs-oper-fc.grib2",
        "scda-fc.grib2",
        "scwv-fc.grib2",
        "waef-ef.grib2",
    ]
    
    for f in files:
        fp = folder / f
        
        # only take control forecasts (cf), not perturbation forecasts (pf)
        # only concerns WAEF
        keys = {} 
        if "waef-ef" in fp.name: keys = {'typeOfLevel': 'fc'}
        if "oper-fc" in fp.name: 
            for k in ['isobaricInhPa','surface','heightAboveGround','entireAtmosphere','meanSea']:
                keys = {'typeOfLevel': k}
                ds = xr.open_dataset(fp, engine="cfgrib", filter_by_keys=keys)
                
                log.critical(ds.attrs)
    
                log.info(rgb.purple, f, f" [{k}]")
                for d in ds.data_vars:
                    log.info(d, ": ", ds[d].attrs["long_name"], " ", ds[d].dims)
            
            continue
        ds = xr.open_dataset(fp, engine="cfgrib", filter_by_keys=keys)
    
        log.info(rgb.purple, f)
        for d in ds.data_vars:
            log.info(d, ": ", ds[d].attrs["long_name"], " ", ds[d].dims)
    
    log.info("end")    
    
    return 