from pathlib import Path

from core.config import Config
from core.static import constraint
from core import log
from core import env

import toml

config_dict = {"General": {}}
cfg_path = Path.cwd() / "harp-config.toml"
if cfg_path.is_file():
    log.debug(f"Harp ingested a local config file {cfg_path}", e=ValueError)
    config_dict = toml.load(cfg_path)

harp_config = Config(config_dict)                           # all data from toml
general_config = Config(harp_config.get_subsection("General")) # just the 'general' subsection

try:
    path_from_env = env.getdir("DIR_ANCILLARY")
except NotADirectoryError:
    path_from_env = None
    

if path_from_env == "None":
    path_from_env = None

default_config = dict(
    dir_storage = path_from_env,
    harmonize = True,
    offline = False,
    verbose_lvl = "debug"
)

default_config_constraints = dict(
    dir_storage     = constraint.path(exists=True, mode="dir"),
    harmonize       = constraint.bool(),
    offline         = constraint.bool(),
    verbose_lvl     = constraint.literal(["error","warning","info","debug"])
)

general_config.ingest(default_config)