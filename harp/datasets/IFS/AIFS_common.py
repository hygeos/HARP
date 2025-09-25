from datetime import datetime, timedelta
from pathlib import Path
import requests

from core.static import abstract, interface
from core import log, rgb
import xarray as xr

from harp.datasets.HarpDataset import HarpDataset



class AIFS_common:
    
    # TODO forecast horizon is available (leadtime available)
    #   -> depends on the datasets (AIFS goes a lot further)    
    
    def _leadtime_is_available(leadtime: int) -> bool:
        """
        Check if leadtime if available for IFS opendata
        """
        if leadtime > 360: return False # goes up ‥ T+360h
        return (leadtime % 6) == 0      # AIFS leadtimes are 6h based
    
    def _time_is_available(time: datetime) -> bool:
        """
        Check if datetime is available for IFS opendata download
        This is common to all IFS datasets
        """        
        
        now = datetime.now()
        delta = now - time 
        
        if delta.days > 3: return False             # too old
        if delta.total_seconds() < 0: return False  # is in the future
        if delta.total_seconds() < 3600 * 12:       # doubt, checking
            times = AIFS_common.parse_available_times(only_today=True)
            return time in times
        return True
    
    def _parse_available_times(only_today=False):
        """
        Parses available times with requests calls and html parsing
        """
        url = "https://data.ecmwf.int/forecasts/"
        
        days = []
        if not only_today:
            req = requests.get(url)
            # TODO check the request status
            
            # parse HTML
            days = req.text.split("\n")
            days = [l for l in days if "forecasts/20" in l]
            days = [l.split("/\">")[1].split("/</a>")[0] for l in days]
            
        elif only_today: # avoid parsing unambigous days
            days = [datetime.now().strftime("%Y%m%d")]
        
        times = []
        for day in days: 
            req = requests.get(url + day)
            # TODO check the request status
            
            # parse html 
            hours = req.text.split("\n")
            hours = [l for l in hours if "z/" in l]
            hours = [l.split("z/\">")[1].split("z/</a>")[0] for l in hours]
            
            time = datetime.strptime(day, "%Y%m%d")
            for h in hours:
                ntime = datetime(time.year, time.month, time.day, int(h))
                times.append(ntime)
            
        return times
    
    def _validate_request(time: datetime, leadtime: int):
        
        ta = AIFS_common._time_is_available(time)
        if not ta:
            message = f"Requested reference time {time} is not available in AIFS."
            log.error(message)
            raise KeyError(message)
        
        lta = AIFS_common._leadtime_is_available(leadtime)
        if not lta: 
            message = f"Requested timelead of {leadtime}h is not available in AIFS."
            log.error(message)
            raise KeyError(message)
    
    def _convert_to_valid_time(time: datetime) -> tuple:
        """
        Helper function to help get valid input
        """
        h, m = time.hour, time.minute
        t = h + m / 60
        if h not in [0, 6, 12, 18] or m != 0:
            log.info(f"Time {h:02}:{m:02} not a valid time for IFS. Must be one of [00:00, 06:00, 12:00, 18:00]")
            
            # convert to closest below
            for rh in [0, 6, 12, 18]:
                if t >= rh:
                    h = rh
                    m = 0
                    log.info(f"Defaulting to time {h:02}:{m:02}.")
                    break
        return h, m
    
    def _convert_to_valid_leadtime(leadtime: int):
        """
        Helper function to help get valid input
        """
        newleadtime = leadtime
        
        if leadtime % 6 != 0:
            newleadtime = ((leadtime // 6) + 1) * 6 # take closest further forecast
            
        if newleadtime > 360:
            log.error("Invalid leadtime: cannot find any further valid leadtime to query", e=ValueError)
        
        return newleadtime    

    def _get_urls(time: datetime, leadtime: int):
        
        assert (leadtime % 6) == 0
        
        base_url = "https://data.ecmwf.int/forecasts/%Y%m%d/%Hz/aifs/0p25/oper/"
        file_name = f"%Y%m%d000000-"
        file_name_suffix = "%sh-oper-fc.grib2"
        
        urls = []
        
        nleadtimes = leadtime // 6
        for i in range(nleadtimes):
            url = time.strftime(base_url) + time.strftime(file_name) + file_name_suffix % (i * 6)
            urls.append(url)
            
        return urls
    
    def _download(time: datetime, leadtime: int):
        pass