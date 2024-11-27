# standard library imports
from datetime import date
from pathlib import Path

# third party imports
from core.cache import cache_json 
import pytest

# sub package imports
from harp.providers.merra2_parser import Merra2Parser


def test_get_versions():
    
    parser = Merra2Parser()
    products = parser.get_products_vers()
    
    assert 'M2I1NXINT' in products
    assert 'M2T1NXRAD' in products
    
    assert products['M2I1NXINT'] == "5.12.4"
    assert products['M2T1NXRAD'] == "5.12.4"


def test_get_specs_local():
    
    parser = Merra2Parser()
    cache_file = Path(__file__).parent / 'inputs' / 'merra2.json'
    specs = cache_json(cache_file, inputs="ignore")(parser.get_model_specs)(date(2012, 12, 10))
    
    assert 'M2I1NXINT' in specs
    assert 'M2T1NXRAD' in specs
    
    assert 'TQV'    in specs['M2I1NXINT']['variables']
    assert 'CLDTOT' in specs['M2T1NXRAD']['variables']
    
    