

from datetime import datetime, date


class AIFS:
    
    class get:
        
        def forecast(variables: list, dt: datetime, leadtime: int, offline=False):
            pass
            
        
        # def day(variables: list, date: date, offline=False):
            # pass
        
    
    def _get_filename(variables: list, dt: datetime, leadtime: int):
        filename = dt.strftime("AIFS_raster__dt_%Y%m%dT%H__") + f"ld_{leadtime}.nc" 
        return filename
    # def _get_forecast(variables: list, dt: datetime, leadtime: int, offline=False):
        
        