from harp.utils.nomenclature import Nomenclature
from core import log
from pathlib import Path

import pytest

raw = Path(__file__).parent / "raw_nomenclature.csv"



def test_init():
    n = Nomenclature(raw)

    
@pytest.mark.parametrize("raw_name", [
    "TOTANGSTR","DUEXTTAU","OCEXTTAU","BCEXTTAU","SSEXTTAU","SUEXTTAU", "CLDTOT",
    "CLDLOW","CLDMID","CLDHGH","TAUTOT","TAULOW","TAUMID","TAUHGH",
])
def test_raw_to_std_no_error(raw_name):
    n = Nomenclature(raw)
    
    n.get_std_name(raw_name)

    
@pytest.mark.parametrize("std_name", [
    "surface_pressure", "sea_level_pressure", "ozone", "water_vapor", 
    "aod_550", "angstr_coef_550", "dust_aod_550", "organic_carbon_aod_550", 
    "black_carbon_aod_550", "sea_salt_aod_550", "sulfate_aod_550", "cloud_cov"
])
def test_std_to_raw_no_error(std_name):
    n = Nomenclature(raw)
    
    n.get_raw_name(std_name)


def test_raw_to_std():
    n = Nomenclature(raw)
    
    assert n.get_std_name("TOTEXTTAU") == "aod_550"
    
    
def test_std_to_raw():
    n = Nomenclature(raw)
    
    assert n.get_raw_name("aod_550") == "TOTEXTTAU"


def test_raw_to_std_fail():
    n = Nomenclature(raw)
    
    with pytest.raises(KeyError):
        n.get_std_name("prout")
        
        
def test_std_to_raw_fail():
    n = Nomenclature(raw)
    
    with pytest.raises(KeyError):
        n.get_raw_name("prout")