import pytest
from harp.datasets import ERA5
from tests.GenericDatasetTester import GenericDatasetTester


def test_metatest():
    
    variables = dict(wind_u = "u", wind_v = "v")
    GenericDatasetTester.test_basic_get(
        ERA5.GlobalReanalysisVolumetric, 
        variables=variables, 
        levels=ERA5.GlobalReanalysisVolumetric.pressure_levels[-5:]
    )