import pytest

from tempfile import TemporaryDirectory
from pathlib import Path
from datetime import datetime

from core import log
from harp.providers.NASA import MERRA2


def test_get():
    
    with TemporaryDirectory() as tmpdir:
        
        merra2 = MERRA2.hourly.M2I1NXASM(
            config = dict(dir_storage = Path(tmpdir)),
            variables = dict(
                surface_pressure = "PS",
                ozone = "TO3",
            ),
        )
        
        ds = merra2.get(
            time = datetime(2012, 12, 12, 17, 15),
        )

        assert "surface_pressure" in ds.data_vars
        assert "ozone" in ds.data_vars
        
        # log.info(ds)