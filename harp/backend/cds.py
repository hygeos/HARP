
from pathlib import Path
import os

import cdsapi

class auth:
    
    def get_client(url):
        
        dotrc = os.environ.get("CDSAPI_RC", os.path.expanduser("~/.cdsapirc"))
        config = cdsapirc.read_config(dotrc) 
    
        key = config['key']
    
        return cdsapi.Client(url=url, key=key)
    

class cdsapirc:


    def read_config(path: Path):
        """
        Read file from path values and parses configuration for CDSAPI
        
        name.var: value
        ex: "cds.url: https://cds.climate.copernicus.eu/api/v2"
            "cds.key: cds.key: 12434:34536a45-45d7-87c6-32a5-as9as8d7f6a9" # fake credentials
        """
        
        config = {}
        
        if isinstance(path, str): path = Path(path)
        if not path.exists(): raise RecursionError(f'Could not find file {path}') 
        
        path = path.resolve()
        f = open(path, 'r')
        
        for line in f.readlines():
            
            if ':' not in line: continue
            left, value = [s.strip() for s in line.strip().split(":", 1)]
            # ex: 'url', ' https://cds.climate.copernicus.eu/api/v2'
            
            if left not in config: config[left] = value     
        f.close()
        
        if len(config) == 0: raise SyntaxError(f'Invalid configuration syntax in file \'{path}\'')
        
        return config