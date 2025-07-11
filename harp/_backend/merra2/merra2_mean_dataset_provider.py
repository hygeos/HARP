from datetime import date, datetime
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


from harp._backend.baseprovider import BaseDatasetProvider
from harp._backend.nomenclature import Nomenclature
from harp._backend import harp_std

warnings.filterwarnings('ignore', message='PyDAP was unable to determine the DAP protocol*')


class Merra2MeanDatasetProvider(BaseDatasetProvider):
    
    keywords = ["NASA", "GMAO"]
    institution = "NASA"
    
    host = 'urs.earthdata.nasa.gov' # server to download from
    auth = auth.get_auth(host)  # credentials from netrc file
    
    
    def __init__(self, collection: str, name: str, **kwargs):
        
        self.name = name
        self.collection = collection
        
        super().__init__(config_subsection="MERRA2", **kwargs)
        
        self.infos_json_path     = Path(__file__).parent / "_layout" / self.collection / "infos"     / f"{self.name}.json"
        self.variables_csv_path  = Path(__file__).parent / "_layout" / self.collection / "variables" / f"{self.name}.csv"
        
        with open(self.infos_json_path, "r") as f: 
            self.infos = json.load(f)
        
        self.nomenclature = Nomenclature(self.variables_csv_path, cols=["harp_name", "query_name", "units", "long_name"], query_column="query_name", context="MERRA2")


    def download(self, variables: list[str], time: datetime, *, area: dict=None, offline=False) -> list[Path]:
        """
        variables are expected to be raw
        """
        time = date(time.year, time.month, time.day)
        
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        local = [] # local path
        query = [] # raw variables which do not exists locally
        
        for v in variables:
            path = self._get_variable_target_path(v, time)
            if path.is_file():
                log.debug(f"Found locally: {path.name}")
                local.append(path)
            else:
                query.append(v)
        
        if query:
            if offline:
                log.error(f"Offline mode is activated and data is missing locally [{', '.join(query)}] for {time.strftime('%Y-%m-%d')}",
                    e=FileNotFoundError)
            log.debug(f"Querying {self.name} for variables {', '.join(query)} on {time}")
            ds = self._retrieve_day(query, time, area)
            retrieved = self._post_process_download(ds, query, time)
            local += retrieved
        
        return local
        
        
    # TODO move to generic backend code for storage etc..
    def _post_process_download(self, ds, query, day):
        
        ret = []
        
        for var in query:
            path: Path = self._get_variable_target_path(var, day)
            vds = ds[[var]].compute()
            path.parent.mkdir(parents=True, exist_ok=True)
            log.debug(f"Retrieved: {path.name}")
            to_netcdf(vds, path, engine="netcdf4")
            ret.append(path)
        
        return ret
    
    
    def _retrieve_day(self, variables: list[str], day: date,  area: dict=None) -> xr.Dataset:
        """
        Download to a temporary dir the whole day, returns the dataset trimmed with OPeNDAP 
        """
        
        url = self._get_url(day)
    
        # backup in case original file doesn't exists -> it has been reprocessed
        url_bis = url.replace('400', '401') # file is reprocessed, not original 
        
        r1 = requests.get(url+'.xml')
        if r1.status_code == 404:
            r2 = requests.get(url_bis+'.xml')
            if r2.status_code == 200:
                url = url_bis
        
        # Download file
        session = requests.Session()
        session = setup_session(self.auth['user'], self.auth['password'], check_url=url)

        store = xr.backends.PydapDataStore.open(url, session=session)
        ds = xr.open_dataset(store)
        
        ds = ds[variables].compute() # trim dataset to only keep desired variables
    
        return ds
    
    
    def _get_variable_target_path(self, variable, day) -> Path:
        
        return self.config.get("dir_storage") / self._get_target_subfolder(day) / self._get_target_filename(variable, day)
    
    
    def _get_target_subfolder(self, day):
        
        subf = day.strftime("%Y") if self.collection == "MERRA2_MONTHLY" else day.strftime("%Y/%m")
        
        return Path() / self.institution / self.collection / self.name / subf
    
    
    def _get_target_filename(self, variable, day, region=False):
        
        filestr = f"{self.collection}_{self.name}_"
        filestr += "region_" if region else "global_"
        filestr += day.strftime("%Y-%m") if self.collection == "MERRA2_MONTHLY" else day.strftime("%Y-%m-%d")
        
        return filestr + f"_{variable}.nc"
        
    
    def _get_url(self, day: date):
        
        base = Path("goldsmr4.gesdisc.eosdis.nasa.gov/opendap/")
        
        url = base / self.collection / Path(str(self.__class__.__name__) + ".5.12.4")
        subdirstr = day.strftime("%Y/%m") if self.collection == "MERRA2" else day.strftime("%Y")
        url = url / subdirstr
        filename = self.infos["generic_name"]
        url = url / day.strftime(filename)
        
        return "https://" + str(url)
        
    
    def _standardize(self, ds):
        
        ds = ds.rename_dims({'lat': harp_std.lat_name, 'lon': harp_std.lon_name}) # rename merra2 names to ECMWF convention
        ds = ds.rename_vars({'lat': harp_std.lat_name, 'lon': harp_std.lon_name})
        
        ds = harp_std.center_longitude(ds, center=harp_std.longitude_center)
        
        return ds