from datetime import timedelta
from pathlib import Path

from core.config import Config
from core.static import constraint
from core import log
from core import env

import harp
import toml

default_config = Config({}) # just the 'general' subsection

try:
    path_from_env = env.getdir("DIR_ANCILLARY")
except NotADirectoryError:
    path_from_env = None
    

if path_from_env == "None":
    path_from_env = None

default_config_dict = dict(
    dir_storage = path_from_env,
    harmonize = True,
    offline = False,
)

default_config_constraints = dict(
    dir_storage     = constraint.path(exists=True, mode="dir"),
    harmonize       = constraint.bool(),
    offline         = constraint.bool(),
    verbose_lvl     = constraint.literal(["error","warning","info","debug"])
)

default_config.ingest(default_config_dict)


class _internal:
    debug = False

if not _internal.debug:
    log.silence(harp, log.lvl.DEBUG)