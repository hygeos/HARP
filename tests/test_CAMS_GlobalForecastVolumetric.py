from tempfile import TemporaryDirectory
import pytest
from harp.datasets import CAMS
from tests.GenericDatasetTester import GenericDatasetTester

from datetime import datetime, timedelta
from pathlib import Path

from harp._backend._utils.HarpErrors import *


def test_metatest():
    
    variables = dict(wind_10u = "u10", wind_10v = "v10")
    GenericDatasetTester.test_basic_get(CAMS.GlobalForecast, variables=variables)
    
    

def test_fail_extended_forecast_not_set():
    
    
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        config = dict(dir_storage = tmpdir)
    
        cams = CAMS.GlobalForecast(
            config = config,
            variables = {"water_vapor" : "tcwv"},
            allow_extended_forecast = False,
        )
        
        # try to query outside 12h leadtime without setting allow_extended_forecast
        with pytest.raises(InvalidQueryError):
            ext = datetime.now() + timedelta(days=2) # inside extended forecast, -> outside of 12 hour leadtime 
            ds = cams.get(time = ext)
            


def test_fail_outside_max_forecast_range():
    
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        config = dict(dir_storage = tmpdir)
    
        cams = CAMS.GlobalForecast(
            config = config,
            variables = {"water_vapor" : "tcwv"},
            allow_extended_forecast = True,
        )
        
        # try to query outside 12h leadtime without setting allow_extended_forecast
        with pytest.raises(InvalidQueryError):
            ext = datetime.now() + timedelta(days=25, hours=3) # inside extended forecast, -> outside of 12 hour leadtime 
            ds = cams.get(time = ext)



def test_ok_forecast_within_range():
    
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        config = dict(dir_storage = tmpdir)
    
        cams = CAMS.GlobalForecast(
            config = config,
            variables = {"water_vapor" : "tcwv"},
            allow_extended_forecast = True,
        )
        
        ext = datetime.now() + timedelta(hours=35) # inside extended forecast, -> outside of 12 hour leadtime 
        ds = cams.get(time = ext)
        
        
