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
path_from_env = env.getvar("HARP_CACHE_DIR", False) or env.getvar("DIR_ANCILLARY", False)

if not path_from_env:
    path_from_env = None

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