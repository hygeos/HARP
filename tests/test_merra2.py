# standard library imports
from tempfile import TemporaryDirectory
from datetime import datetime, date
from pathlib import Path

# third party imports
from core.floats import feq
import numpy as np
import pytest

# sub package imports
from harp import MERRA2


def test_get_datetime():
    """
    Test basic get, either from local file or download
    Test variable name nomenclature
    """
    
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
    
        merra = MERRA2(
                    model=MERRA2.models.M2T1NXRAD,
                    directory=tmpdir,
                    config_file=Path('tests/inputs/merra2.json'),
                    )
        
        ds = merra.get(variables=['cloud_cov', 'cloud_od'], dt=datetime(2023, 9, 10, 22, 35))
        
        # check that the variables have been correctly renamed
        variables = list(ds)
        assert 'CLDTOT' not in variables
        assert 'TAUTOT' not in variables
        
        # test wrap
        assert feq(np.max(ds.longitude.values),  180.0)
        assert feq(np.min(ds.longitude.values), -180.0)
        
        # check that the time interpolation occured
        assert len(np.atleast_1d(ds.time.values)) == 1


def test_get_date():
    """
    Test basic get, either from local file or download
    Test variable name nomenclature
    """
    
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
    
        merra = MERRA2(
                    model=MERRA2.models.M2T1NXRAD,
                    directory=tmpdir,
                    config_file=Path('tests/inputs/merra2.json'),
                    )
        
        ds = merra.get_day(variables=['cloud_cov', 'cloud_od'], date=date(2023, 9, 10))
        
        # check that the variables have been correctly renamed
        variables = list(ds)
        assert 'CLDTOT' not in variables
        assert 'TAUTOT' not in variables
        
        # test wrap
        assert feq(np.max(ds.longitude.values),  180.0)
        assert feq(np.min(ds.longitude.values), -180.0)
        
        # check that the time interpolation occured
        assert len(np.atleast_1d(ds.time.values)) == 24


def test_get_local_var_def_file():
    """
    Test basic get, either from local file or download
    Test variable name nomenclature
    """
    
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
    
        merra = MERRA2(model=MERRA2.models.M2I1NXASM,
                    config_file=Path('tests/inputs/merra2.json'), 
                    directory=tmpdir,
                    nomenclature_file=Path('tests/inputs/nomenclature/variables.csv')
                    )
        
        ds = merra.get(variables=['local_total_column_ozone'], dt=datetime(2023, 9, 10, 13, 35))
        
        # check that the variables have been correctly renamed
        variables = list(ds)
        assert 'TO3' not in variables
        assert 'local_total_column_ozone' in variables


def test_no_std():
    """
    Test basic usage, either from local file or download
    but without name standardization phase
    """
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
    
        merra = MERRA2(model=MERRA2.models.M2T1NXRAD,
                       config_file=Path('tests/inputs/merra2.json'), 
                       directory=tmpdir,
                       no_std=True,
                    )
        
        ds = merra.get(variables=['CLDTOT'], dt=datetime(2023, 9, 10, 13, 35))
        
        # check that the variables have not changed and kept their original short names
        variables = list(ds)
        
        assert 'CLDTOT' in variables
        assert 'total_cloud_cover' not in variables


def test_fail_get_offline():
    
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        merra = MERRA2(model=MERRA2.models.M2T1NXRAD,
                       config_file=Path('tests/inputs/merra2.json'), 
                       directory=tmpdir,
                       offline=True,
                       )
        
        with pytest.raises(ResourceWarning):
            merra.get(variables=['cloud_cov', 'cloud_od'], 
                      dt=datetime(2001, 9, 11, 13, 35))


def test_download_offline():
    merra = MERRA2(model=MERRA2.models.M2I1NXINT,
                   config_file=Path('tests/inputs/merra2.json'), 
                   directory=Path('tests/inputs/MERRA2/'),
                   offline=True,
                   )
            
    f = merra.download(variables=['water_vapor'], d=date(2023, 9, 10))

    assert isinstance(f, Path)
    
    
def test_fail_download_offline():
    merra = MERRA2(model=MERRA2.models.M2I1NXINT,
                   config_file=Path('tests/inputs/merra2.json'), 
                   directory=Path('tests/inputs/MERRA2/'),
                   offline=True,
                   )
    
    with pytest.raises(ResourceWarning):
        merra.download(variables=['water_vapor'], d=date(2001, 9, 10))


    