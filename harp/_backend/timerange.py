

from datetime import date, datetime, timedelta
from typing import Callable, Collection, Literal

import numpy as np

from core.static import interface
from core import log


class Timerange:
    """
    Generic helper class which encapsulate small logic to determine wheter the datetime
    is present in the Timerange defined in the constructor
    """
    
    def __init__(self, start: datetime|timedelta|Callable, end: datetime|timedelta|Callable):
        
        now = datetime.now()
        
        # apply delta if provided time is relative (ex: end = -5days)
        if isinstance(start, timedelta):
            s = now + start
        if isinstance(end, timedelta):
            e = now + end
        
        # apply functions to compute start and end if provided
        if isinstance(start, Callable):
            s = start(now)
        if isinstance(end, Callable):
            e = end(now)

        if s > e:
            log.error("Invalid Timerange parameters: Start cannot be >= End", e=ValueError)

        self.start = s
        self.end = e
        
    
    def is_available(self, time: datetime):
        
        return self.start <= time <= self.end  