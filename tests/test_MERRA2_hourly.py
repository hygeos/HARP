import pytest
from harp.datasets import MERRA2
from tests.GenericDatasetTester import GenericDatasetTester



def test_metatest():
    
    variables = dict(wind_10u = "U2M", wind_10v = "V2M")
    GenericDatasetTester.test_basic_get(MERRA2.M2I1NXASM, variables=variables)