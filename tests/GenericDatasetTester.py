from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Type

import pytest
import numpy as np
import xarray as xr

from harp._backend import harp_std
from harp._backend.baseprovider import BaseDatasetProvider
from harp.utils import Computable

from core import log

class GenericDatasetTester:
    
    # TODO: add generic meta test for forecasts datasets
    def test_basic_get(DatasetProvider: Type[BaseDatasetProvider], **kwargs):
        """
        Basic testing of get
            - [x] normal get
            - [x] try offline (before cache [crashes] and after cache)
            - [x] computables
            - [x] renaming
            - [x] lat lon values
        """
        
        assert "variables" in kwargs
        variables: dict = kwargs.pop("variables", None)  
        assert type(variables) == dict 
        assert len(variables.keys()) >= 2, "Test [Computable] requires at least to variables, with same dimensionnality"
        
        
        with TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            config = dict(dir_storage = tmpdir)
            
            # Instantiate the dataset provider with a temporary directory 
            datasetprovider: BaseDatasetProvider = DatasetProvider(config=config, variables=variables)
        
            time = datasetprovider.timerange.end - timedelta(days=25)
            
            # test [1]: offline mode no cache ------------------------------------
            with pytest.raises(FileNotFoundError):
                datasetprovider.get(time, offline=True, **kwargs) 
                
            # test [2]: standard online mode -------------------------------------
            ds = datasetprovider.get(time, offline=False, **kwargs) 
            
            # test[3]: offline mode with cache -----------------------------------
            ds = datasetprovider.get(time, offline=True, **kwargs) 
    
            # test[4]: renaming --------------------------------------------------
            qvars = list(variables.values())
            nvars = list(variables.keys())
            
            for nv in nvars: assert nv not in qvars
            for qv in qvars: assert qv not in ds.data_vars
            
            assert harp_std.lat_name  in list(ds.dims)
            assert harp_std.lon_name  in list(ds.dims)
            assert harp_std.time_name in list(ds.dims)
            
            # test [5]: computable variables -------------------------------------
            x, y   = qvars[0], qvars[1]  
            nx, ny = nvars[0], nvars[1]
            
            variables["computable"] = Computable(operands=qvars, func=lambda ds: ds[x] * ds[y], keep_operands=True) 
            
            # Re-Instantiate the dataset provider with a temporary directory 
            datasetprovider: BaseDatasetProvider = DatasetProvider(config=config, variables=variables)            
            ds = datasetprovider.get(time, offline=True, **kwargs) # operands already downloaded -> offline = True
            
            assert "computable" in ds.data_vars
            np.testing.assert_allclose(ds["computable"], ds[ny] * ds[nx], rtol=1e-9, atol=1e-9)
            
            # test [6]: lat lon values -------------------------------------------
            maxlon = np.nanmax(ds[harp_std.lon_name])
            minlon = np.nanmin(ds[harp_std.lon_name])
            
            assert -180.01 < minlon < 180.01
            assert -180.01 < maxlon < 180.01
            
            maxlat = np.nanmax(ds[harp_std.lat_name])
            minlat = np.nanmin(ds[harp_std.lat_name])
            
            assert -90.01 < minlat < 90.01
            assert -90.01 < maxlat < 90.01
