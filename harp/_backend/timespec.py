from datetime import date, datetime, timedelta

import numpy as np

from core import log

class RegularTimespec:
    """
    Generic helper class to describe timesteps of a dataset which contains intra-day timesteps
    Allow to get the encompassing timesteps of any queries
    """
    
    def __init__(self, start: timedelta, count: int):
        self.start = start
        self.count = count
        self.dt = timedelta(days=1) / count
        self.intraday_timesteps = [start + i*self.dt for i in range(self.count)]
        
        self.midnight_start = (start == timedelta(seconds=0)) # True if first timestep is 00:00 False otherwise
            
    
    def get_encompassing_timesteps(self, time: datetime) -> list[datetime, datetime]:
        """Returns the datetimes bound of the specs, around the provided time
        If the time is exactly on a timestep, only that timestep is returned
        """
        
        assert type(time) == datetime

        day = datetime(time.year, time.month, time.day)
        intra_day_time = time - day
        
        timesteps = np.array(self.intraday_timesteps)
       
        upper_i = np.searchsorted(timesteps, intra_day_time, side="left")
        lower_i = upper_i - 1
        
        offset = 0
        
        if lower_i < 0:
            timesteps = np.concatenate([timesteps-timedelta(days=1), timesteps])
            offset = self.count
            upper_i += offset
            lower_i += offset
            
        if upper_i >= self.count:
            timesteps = np.concatenate([timesteps, timesteps+timedelta(days=1)])
            
        if intra_day_time == timesteps[upper_i]: # exact timestep provider
            return timesteps[[upper_i]] + day 
        
        return timesteps[[lower_i, upper_i]] + day
    
    
    def get_encompassing_timesteps_range(self, times: list[datetime, datetime]):
        
        if not len(times) == 2:
            log.error("Expected Collection of at least 2 datetimes", e=RuntimeError)
            
    # @interface
    def get_complete_day(self, day: date):
        day = datetime(day.year, day.month, day.day)
        return np.array(self.intraday_timesteps) + day
    
    # def aggregate_per_day(self, timesteps: list[datetime], complete_days=False):
        
    #     days = {}
        
    #     for ts in timesteps:
    #         day = ts.date()
            
    #         if day not in days: 
    #             days[day] = []
            
    #         if ts not in days[day]:
    #             days[day].append(ts)
        
    #     if complete_days:
    #         for day in days:
    #             days[day] = self.get_complete_day(day)
                
    #     return days