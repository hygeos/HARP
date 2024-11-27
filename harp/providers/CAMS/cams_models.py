from core.fileutils import filegen

from datetime import date
from pathlib import Path

import cdsapi

class CAMS_Models:
        
    @filegen(1)
    def global_atmospheric_composition_forecast(cams, target: Path, d: date, area):
        """
        Download a single file, containing 24 times, hourly resolution
        uses the CDS API. Uses a temporary file and avoid unnecessary download 
        if it is already present, thanks to fileutil.filegen 
        
            model example:   
        https://ads.atmosphere.copernicus.eu/cdsapp#!/dataset/cams-global-atmospheric-composition-forecasts
        https://confluence.ecmwf.int/display/CKB/CAMS%3A+Global+atmospheric+composition+forecast+data+documentation#heading-Table1SinglelevelFastaccessparameterslastreviewedon02Aug2023
        
        
        - cams: CAMS instance object to be passed
        - target: path to the target file after download
        - d: date of the dataset
        """
        
        
        dataset = 'cams-global-atmospheric-composition-forecasts'
        d_str = d.strftime("%Y-%m-%d")
        request = {
                'variable': cams.ads_variables,
                'date': [f"{d_str}/{d_str}"], # ['2024-08-08/2024-08-08']
                'time': ['00:00', '12:00'],
                'leadtime_hour': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11'],
                'type': ['forecast'],
                'data_format': 'netcdf',
            }
        
        if area is not None: 
            request['area'] = area
        
        cams.client.retrieve(dataset, request, target)