from datetime import timedelta
from pathlib import Path

from core.config import Config
from core.static import constraint
from core import log
from core import env

import harp
import toml

default_config = Config({}) # just the 'general' subsection

# cascading source for value
# HARP_CACHE_DIR > DIR_ANCILLARY > DIR_DATA/ancillary
# else: False >>> require user to set dir_storage in constructor
path_from_env = env.getvar("HARP_CACHE_DIR", default=env.getvar("DIR_ANCILLARY", default=False))

if path_from_env == False:
    path_from_env = env.getvar("DIR_DATA", default=False)
    if path_from_env != False:
        path_from_env = Path(path_from_env) / "ancillary"

if path_from_env:
    if not Path(path_from_env).exists():
        log.error("HARP_CACHE_DIR or DIR_ANCILLARY path does not exist, please create it: ", path_from_env)

if not path_from_env:
    path_from_env = None
    
# log.debug(log.rgb.red, path_from_env)

default_config_dict = dict(
    dir_storage = path_from_env,
    harmonize = True,
    offline = False,
    lock_timeout = -1, # in seconds
    lock_lifetime = timedelta(days=1),
)

default_config.ingest(default_config_dict)

_debug = bool(int(env.getvar("HARP_DEBUG", False)))

if not _debug:
    log.silence(harp, log.lvl.DEBUG)