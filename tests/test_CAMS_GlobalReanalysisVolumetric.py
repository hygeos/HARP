import pytest
from harp.datasets import CAMS
from tests.GenericDatasetTester import GenericDatasetTester


def test_metatest():
    
    variables = dict(wind_u = "u", wind_v = "v")
    GenericDatasetTester.test_basic_get(
        CAMS.GlobalReanalysisVolumetric, 
        variables=variables, 
        levels=CAMS.GlobalReanalysisVolumetric.pressure_levels[-5:]
    )