from datetime import datetime


class CdsAtomicQuery:
    
    def __init__(self, *,
        variables: str, 
        day: datetime,
        times: datetime|list[datetime],
        area: dict = None,
        levels: list[int] = None,
    ):
    
        self.variables = variables
        self.day      = day
        self.times    = times
        self.area     = area
        self.levels   = sorted(levels)