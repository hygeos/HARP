# standard library imports
from pathlib import Path

# third party imports
import pytest

# sub package imports
from harp.providers.IFS.test import read_ifs


def test_read_grib2():
    
    read_ifs()