import pytest
from harp.datasets import ERA5
from tests.GenericDatasetTester import GenericDatasetTester



def test_metatest():
    
    variables = dict(wind_10u = "u10", wind_10v = "v10")
    GenericDatasetTester.test_basic_get(ERA5.GlobalReanalysis, variables=variables)