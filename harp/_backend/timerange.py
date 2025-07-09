

from datetime import date, datetime, timedelta
from typing import Callable, Collection, Literal

import numpy as np

from core.static import interface
from core import log


class timerange:
    """
    Generic helper class which encapsulate small logic to determine wheter the datetime
    is present in the Timerange defined in the constructor
    
    [TODO] used as a possible parameter for the get method of providers
    """
    
    def __init__(self, start: datetime|timedelta, end: datetime|timedelta):
        
        now = datetime.now()
        
        # consider time relative to now if both are timedelta (ex: end = -5days)
        if isinstance(start, timedelta) and isinstance(end, timedelta):
            s = now + start
            e = now + end
        # consider end relative to start
        if isinstance(start, datetime) and isinstance(end, timedelta):
            s = start
            e = start + end
        # consider start relative to end
        if isinstance(start, timedelta) and isinstance(end, datetime):
            s = end + start
            e = end
        
        if s > e:
            log.error("Invalid Timerange parameters: Start cannot be >= End", e=ValueError)

        self.start = s
        self.end = e
        
    
    def contains(self, time: datetime):
        
        return self.start <= time <= self.end  