
# describe variable repartition between groups
models = {
    
    "SCDA" : [ 
        "tp",        # Total precipitation('latitude', 'longitude')
        "v10",       # 10 metre V wind component('latitude', 'longitude')
        "q",         # Specific humidity('isobaricInhPa', 'latitude', 'longitude')
        "sot",       # Soil temperature('soilLayer', 'latitude', 'longitude')
        "mucape",    # Most-unstable CAPE('latitude', 'longitude')
        "ssrd",      # Surface short-wave (solar) radiation downwards('latitude', 'longitude')
        "w",         # Vertical velocity('isobaricInhPa', 'latitude', 'longitude')
        "gh",        # Geopotential height('isobaricInhPa', 'latitude', 'longitude')
        "t",         # Temperature('isobaricInhPa', 'latitude', 'longitude')
        "r",         # Relative humidity('isobaricInhPa', 'latitude', 'longitude')
        "d",         # Divergence('isobaricInhPa', 'latitude', 'longitude')
        "vo",        # Vorticity (relative)('isobaricInhPa', 'latitude', 'longitude')
        "lsm",       # Land-sea mask('latitude', 'longitude')
        "v",         # V component of wind('isobaricInhPa', 'latitude', 'longitude')
        "skt",       # Skin temperature('latitude', 'longitude')
        "u",         # U component of wind('isobaricInhPa', 'latitude', 'longitude')
        "sithick",   # Sea ice thickness('latitude', 'longitude')
        "ptype",     # Precipitation type('latitude', 'longitude')
        "strd",      # Surface long-wave (thermal) radiation downwards('latitude', 'longitude')
        "ssr",       # Surface net short-wave (solar) radiation('latitude', 'longitude')
        "tprate",    # Total precipitation rate('latitude', 'longitude')
        "str",       # Surface net long-wave (thermal) radiation('latitude', 'longitude')
        "asn",       # Snow albedo('latitude', 'longitude')
        "ttr",       # Top net long-wave (thermal) radiation('latitude', 'longitude')
        "vsw",       # Volumetric soil moisture('soilLayer', 'latitude', 'longitude')
        "ewss",      # Time-integrated eastward turbulent surface stress('latitude', 'longitude')
        "nsss",      # Time-integrated northward turbulent surface stress('latitude', 'longitude')
        "msl",       # Mean sea level pressure('latitude', 'longitude')
        "sve",       # Eastward surface sea water velocity('latitude', 'longitude')
        "ro",        # Runoff('latitude', 'longitude')
    ],
    
    "SCWV" : [ 
        "mp2",      # Mean zero-crossing wave period ('latitude', 'longitude')
        "swh",      # Significant height of combined wind waves and swell ('latitude', 'longitude')
        "mwd",      # Mean wave direction ('latitude', 'longitude')
        "pp1d",     # Peak wave period ('latitude', 'longitude')
        "mwp",      # Mean wave period ('latitude', 'longitude')
    ],
    
    
# {'typeOfLevel': 'isobaricInhPa'}
# {'typeOfLevel': 'surface'}
# {'typeOfLevel': 'heightAboveGround'}
# {'typeOfLevel': 'entireAtmosphere'}
# {'typeOfLevel': 'meanSea'}
    
    "AIFS": {
        "isobaricInhPa": [
            "w", # Vertical velocity ('isobaricInhPa', 'latitude', 'longitude')
            "z", # Geopotential ('isobaricInhPa', 'latitude', 'longitude')
            "q", # Specific humidity ('isobaricInhPa', 'latitude', 'longitude')
            "v", # V component of wind ('isobaricInhPa', 'latitude', 'longitude')
            "t", # Temperature ('isobaricInhPa', 'latitude', 'longitude')
            "u", # U component of wind ('isobaricInhPa', 'latitude', 'longitude')
        ],
        
        "surface": [
            "cp",   # Convective precipitation ('latitude', 'longitude')
            "sp",   # Surface pressure ('latitude', 'longitude')
            "skt",  # Skin temperature ('latitude', 'longitude')
            "lsm",  # Land-sea mask ('latitude', 'longitude')
            "tp",   # Total precipitation ('latitude', 'longitude')
            "z",    # Geopotential ('latitude', 'longitude')
        ],
        
        "heightAboveGround": [
            "t2m", # 2 metre temperature ('latitude', 'longitude')
            "d2m", # 2 metre dewpoint temperature ('latitude', 'longitude')
        ],
        
        "entireAtmosphere": [
            "tcw", # Total column water ('latitude', 'longitude')
        ],
        
        "meanSea": [
            "msl", # Mean sea level pressure ('latitude', 'longitude')
        ],
    }
    
    # Ensemble Forecast not interfaced yet -> /!\ VERY LARGE amount of data for ENFO (~ 4GB per timestep)
    # "WAEF" : [ 
        # mwp: Mean wave period('latitude', 'longitude')
        # mp2: Mean zero-crossing wave period('latitude', 'longitude')
        # swh: Significant height of combined wind waves and swell('latitude', 'longitude')
        # mwd: Mean wave direction('latitude', 'longitude')
        # pp1d: Peak wave period('latitude', 'longitude')
    # ],
    
    # "ENFO" : [ 
        # "",
    # ],
}