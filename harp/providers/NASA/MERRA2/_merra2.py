from datetime import date
from pathlib import Path
import json
import warnings

from pydap.cas.urs import setup_session
import xarray as xr
from core import log
from core.static import interface
from core.fileutils import filegen
from core.save import to_netcdf
from core import auth

import requests


from harp.utils.dataset_provider import HarpDatasetProvider
from harp.utils.nomenclature import Nomenclature
from harp.utils import std

import harp.config

warnings.filterwarnings('ignore', message='PyDAP was unable to determine the DAP protocol*')


class BaseMerra2(HarpDatasetProvider):
    
    institution = "NASA"
    
    host = 'urs.earthdata.nasa.gov' # server to download from
    auth = auth.get_auth(host)  # credentials from netrc file
    
    @classmethod
    def init_class(cls, model):
        cls.name = cls.__name__
        cls.model = model
                
        cls.infos_json_path     = Path(__file__).parent / "_layout" / model / "infos"     / f"{cls.name}.json"
        cls.variables_csv_path  = Path(__file__).parent / "_layout" / model / "variables" / f"{cls.name}.csv"
        
        with open(cls.infos_json_path, "r") as f: 
            cls.infos = json.load(f)
        
        cls.nomenclature = Nomenclature(cls.variables_csv_path)

    @classmethod  
    def download(cls, variables: list[str], day: date, *, area: None=None, offline=False) -> list[Path]:
        """
        variables are expected to be raw
        """
        
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        local = [] # local path
        query = [] # raw variables which do not exists locally
        
        for v in variables:
            path = cls._get_variable_target_path(v, day)
            if path.is_file():
                log.debug(f"Found locally: {path.name}")
                local.append(path)
            else:
                query.append(v)
        
        if query:
            if offline:
                log.error(f"Missing data locally [{', '.join(query)}] for {day.strftime('%Y-%m-%d')} while in offline mode",
                    e=FileNotFoundError)
            log.debug(f"Querying {cls.name} for variables {', '.join(query)} on {day}")
            ds = cls._retrieve_day(query, day, area)
            retrieved = cls._post_process_download(ds, query, day)
            local += retrieved
        
        return local
        
        
    @classmethod
    def _post_process_download(cls, ds, query, day):
        
        ret = []
        
        for var in query:
            path: Path = cls._get_variable_target_path(var, day)
            vds = ds[[var]].compute()
            path.parent.mkdir(parents=True, exist_ok=True)
            log.debug(f"Retrieved: {path.name}")
            to_netcdf(vds, path, engine="netcdf4")
            ret.append(path)
        
        return ret
    
    @classmethod
    def _retrieve_day(cls, variables: list[str], day: date,  area: None=None) -> xr.Dataset:
        """
        Download to a temporary dir the whole day, returns the dataset trimmed with OPeNDAP 
        """
        
        url = cls._get_url(day)
    
        # backup in case original file doesn't exists -> it has been reprocessed
        url_bis = url.replace('400', '401') # file is reprocessed, not original 
        
        r1 = requests.get(url+'.xml')
        if r1.status_code == 404:
            r2 = requests.get(url_bis+'.xml')
            if r2.status_code == 200:
                url = url_bis
        
        # Download file
        session = requests.Session()
        session = setup_session(cls.auth['user'], cls.auth['password'], check_url=url)

        store = xr.backends.PydapDataStore.open(url, session=session)
        ds = xr.open_dataset(store)
        
        ds = ds[variables].compute() # trim dataset to only keep desired variables
    
        return ds
    
    @classmethod
    def _get_variable_target_path(cls, variable, day) -> Path:
        
        return harp.config.get("dir_storage") / cls._get_target_subfolder(day) / cls._get_target_filename(variable, day)
    
    @classmethod
    def _get_target_subfolder(cls, day):
        
        subf = day.strftime("%Y") if cls.model == "MERRA2_MONTHLY" else day.strftime("%Y/%m")
        
        return Path() / cls.institution / cls.model / cls.name / subf
    
    @classmethod
    def _get_target_filename(cls, variable, day, region=False):
        
        filestr = f"{cls.model}_{cls.name}_"
        filestr += "region_" if region else "global_"
        filestr += day.strftime("%Y-%m") if cls.model == "MERRA2_MONTHLY" else day.strftime("%Y-%m-%d")
        
        return filestr + f"_{variable}.nc"
        
    @classmethod
    def _get_url(cls, day: date):
        
        base = Path("goldsmr4.gesdisc.eosdis.nasa.gov/opendap/")
        
        url = base / cls.model / Path(str(cls.__name__) + ".5.12.4")
        subdirstr = day.strftime("%Y/%m") if cls.model == "MERRA2" else day.strftime("%Y")
        url = url / subdirstr
        filename = cls.infos["generic_name"]
        url = url / day.strftime(filename)
        
        return "https://" + str(url)
        
    
    @classmethod
    def _standardize(cls, ds):
        
        ds = ds.rename_dims({'lat': std.lat_name, 'lon': std.lon_name}) # rename merra2 names to ECMWF convention
        ds = ds.rename_vars({'lat': std.lat_name, 'lon': std.lon_name})
        
        ds = std.center_longitude(ds, center=std.longitude_center)
        
        return ds