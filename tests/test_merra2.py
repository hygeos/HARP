import pytest

from tempfile import TemporaryDirectory
from pathlib import Path
from datetime import date

from core import log
from harp.providers.NASA import MERRA2


def test_get():
    
    with TemporaryDirectory() as tmpdir:
        
        variables=["surface_pressure", "ozone", "sea_level_pressure"]
        
        merra2 =  MERRA2.hourly.raster.M2I1NXASM(
            dir_storage = Path(tmpdir) # inject tmp dir as storage folder
        )
        
        ds = merra2.get(
            variables = variables,
            time = date(2012, 12, 12),
        )

        for var in variables:
            assert var in ds.data_vars
        
        log.info(ds)