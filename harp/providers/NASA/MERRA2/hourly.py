from datetime import datetime, timedelta

from harp.backend.merra2.merra2_hourly_dataset_provider import Merra2HourlyDatasetProvider
from harp.backend.timespec import RegularTimespec

model = "MERRA2"

class M2I1NXASM(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2I1NXINT(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2I1NXLFO(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2I3NXGAS(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 8)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2SDNXSLV(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 1)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXADG(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXAER(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXCHM(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXCSP(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXFLX(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXINT(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXLFO(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXLND(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXOCN(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXRAD(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T1NXSLV(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
        
class M2T3NXGLC(Merra2HourlyDatasetProvider):
    timespecs = RegularTimespec(timedelta(seconds=0), 8)
    def __init__(self, *args, **kwargs):
        super().__init__(collection=model, name=self.__class__.__name__, *args, **kwargs)
            

