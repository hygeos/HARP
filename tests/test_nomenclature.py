# standard library imports
# ...

# third party imports

import pytest

# sub package imports
from harp.nomenclature import Nomenclature


def test_get_var_std_name():
    
    # test for MERRA2
    nm = Nomenclature(provider = 'MERRA2')
    
    assert nm.get_new_name('SPEED') == 'surf_wind'
    assert nm.get_new_name('TQV')   == 'water_vapor'
    
    # test for CAMS
    nm_cams = Nomenclature(provider = 'CAMS')
    
    assert nm_cams.get_new_name('aod550')   == 'aod_550nm'
    assert nm_cams.get_new_name('suaod550') == 'sulfate_aod_550nm'


def test_get_var_std_name_local_file():

    nm = Nomenclature(provider = 'MERRA2', csv_file='tests/inputs/nomenclature/variables.csv')

    assert nm.get_new_name('SPEED') == 'local_surface_wind_speed'


def test_fail_get_inexistant_var():
    
    # test for MERRA2
    nm = Nomenclature(provider = 'MERRA2')
    
    with pytest.raises(KeyError) as excinfo:
        nm.get_new_name('SPEEEEED')
        nm.get_new_name('NOTAVAR')