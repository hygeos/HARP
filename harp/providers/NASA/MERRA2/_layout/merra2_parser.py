# standard library imports
from datetime import date
import subprocess

# third party imports
from pydap.cas.urs import setup_session
from pydap.client import open_url
import xarray as xr
from core import auth
from core import log, rgb
    
import json
from pathlib import Path
import requests

import warnings
warnings.filterwarnings('ignore', message='PyDAP was unable to determine the DAP protocol*')


cred = auth.get_auth("urs.earthdata.nasa.gov")
burl = "https://goldsmr4.gesdisc.eosdis.nasa.gov/opendap/"


def get_datasets(url):
        """
        Parses the available datasets in an OPeNDAP MERRA2 url
        - Really fast
        """
        cmd = f"curl -s {url} | grep 'contents.html\">M2' | cut -d '>' -f 2 | cut -d '/' -f 1"
        status, output = subprocess.getstatusoutput(cmd) # execute shell cmd
        
        res =  [item.split('.', 1) for item in output.split('\n')]
        
        # transform list of list to a dictionnary
        return {item[0]: item[1] for item in res}


def get_parametrized_url(url):
    """
    Return an URL with full correct path to a dataset, parametrized with date (%s)
    """
    d = date(2012, 12, 12)
    if not url.endswith("/"): url += "/"
    
    param_url = "%Y/" if ("_DIU" in url or "_MON" in url)  else "%Y/%m/"
    
    url = d.strftime(url + param_url)
    
    cmd = f"curl -s {url} | grep -E '.*\"name\": \"MERRA2'"                 # only get html lines where we can extract the filename
    status, output = subprocess.getstatusoutput(cmd)                        # execute request

    if status != 0:
        log.error("Invalid request, skipping url")
        return "invalid", ""

    file_name = output.strip().split('\n')[0].split(' ')[1].split('\"')[1]  # parsing
    
    parts = file_name.split(".")                                            # extract the date
    param = "%Y%m" if ("_DIU" in url or "_MON" in url)  else "%Y%m%d" 
    parametrized_name = parts[0] + "." + parts[1] + f".{param}." + parts[3]            # replace it by generic '%s'
    
    return url, parametrized_name


def extract_dataset_infos(url, model, name, generic_name, skip_present=False):
    base_folder = Path(__file__).parent / model
    json_folder = base_folder / "infos"
    json_folder.mkdir(exist_ok=True)
    
    csv_folder = base_folder / "variables"
    csv_folder.mkdir(exist_ok=True)
    
    json_target = json_folder / f"{name}.json"
    csv_target = csv_folder / f"{name}.csv"
    
    if skip_present and json_target.is_file() and csv_target.is_file():
        log.debug(rgb.blue, f"skipping {name}: already processed files")
        return
    
    
    username = cred['user']
    password = cred['password']
    infos = {}
    variables = {}
    
    log.info(f"Retrieving informations from {url}")
    
    # Download file
    session = requests.Session()
    session = setup_session(username, password, check_url=url)

    store = xr.backends.PydapDataStore.open(url, session=session)
    ds = xr.open_dataset(store)

    name = ds.attrs["ShortName"]

    dimensions = dict(ds.sizes)
    if "lat" in dimensions: dimensions["latitude"]  = dimensions.pop("lat")
    if "lon" in dimensions: dimensions["longitude"] = dimensions.pop("lon")
    
    dims = list(dimensions)

    infos["long_name"] = ds.attrs["Title"]
    infos["short_name"] = ds.attrs["ShortName"]
    infos["generic_name"] = generic_name
    infos["source"] = ds.attrs["Institution"]
    infos["version"] = ds.attrs["VersionID"]
    infos["doi"] = ds.attrs["identifier_product_doi"]
    infos["doi_authority"] = ds.attrs["identifier_product_doi_authority"]
    infos["coverage"] = ds.attrs["SpatialCoverage"]
    infos["spatial_degrees"] = ds.attrs["DataResolution"]
    infos["data_resolution"] = dimensions
    infos["southernmost_lat"] = ds.attrs["SouthernmostLatitude"]
    infos["northernmost_lat"] = ds.attrs["NorthernmostLatitude"]
    infos["westernmost_lon"] = ds.attrs["WesternmostLongitude"]
    infos["easternmost_lon"] = ds.attrs["EasternmostLongitude"]
    infos["dimensions"] = dims
    infos["timestep_start"] = ds.attrs["RangeBeginningTime"]
    infos["timestep_end"]   = ds.attrs["RangeEndingTime"]
    infos["date_start"]     = ds.attrs["RangeBeginningDate"]
    infos["date_end"]       = ds.attrs["RangeEndingDate"]
    
    for v in ds.data_vars:
        
        if "__ENSEMBLE__" in ds[v].attrs["long_name"]:
            log.warning(v.ljust(20), " dims ", ds[v].dims)
        
        variables[v] = {
            "long_name": ds[v].attrs["long_name"].replace("__ENSEMBLE__", ""),
            "units": ds[v].attrs["units"],
        }
    
    ds.close()
        
    # Write parsed info
    # Write json dataset meta infos
    
    with open(json_target, 'w') as f:
        log.info(f"Writing {json_target}")
        json.dump(infos, f, indent=4)
    
    
    # Write contained variables as csv file
    pad = 30
    data = "harp_name, ".ljust(pad) + "raw_name,".ljust(20) + "units, ".ljust(pad) + "long_name\n"
    for v in variables:
        vi = variables[v]
        data += ", ".ljust(pad) + f"{v},".ljust(20) + f"{vi['units']},".ljust(pad) + f"{vi['long_name']}\n"
        
    with open(csv_target, 'w') as f:
        log.info(f"Writing {csv_target}")
        f.write(data)

models = [
        "MERRA2/",
        "MERRA2_DIURNAL/",
        "MERRA2_MONTHLY/",
]

for model in models:
    url = burl + model
    datasets = get_datasets(url)
    
    log.debug(rgb.purple, f"Parsing model {model}")
    
    for name, version in datasets.items():

        u = url + name + "." + version + "/"
        d = date(2012, 12, 12)

        u, g = get_parametrized_url(u)
        u = u + g
        if u == "invalid": continue
        u = d.strftime(u)
        extract_dataset_infos(u, model, name, g, skip_present=False)