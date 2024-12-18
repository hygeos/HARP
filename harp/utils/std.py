# standard library imports
# ...

# third party imports
from typing import Literal
import xarray as xr

# sub package imports

time_name = "time"
lat_name  = "latitude"
lon_name  = "longitude"
longitude_center = 0 # should be either 0 or 180


def center_longitude(ds: xr.Dataset, center:Literal[0, 180]=0):
    """
    Center longitudes from [0, 360] to [-180, 180] or from [-180, 180] to [0, 360]
    """
    
    assert (center == 0.0) or (center == 180.0)
    
    lon = None
    if center == 0.0:
        lon = (ds[lon_name].values + 180) % 360 - 180
    elif center == 180.0:
        lon = (ds[lon_name].values) % 360
    
    ds = ds.assign_coords({lon_name:lon})
    ds = ds.sortby(lon_name)
    return ds
