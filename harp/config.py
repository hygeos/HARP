from pathlib import Path
from core.config import Config
from core import log

import harp

cfg_path = Path.cwd() / "harp-config.toml"
if not cfg_path.is_file():
    log.error("Harp requires a local config file", e=ValueError)


config = Config(cfg_path)

if not config.get(key="dir_storage").is_dir():
    log.error("Harp requires an existing folder as storage", e=ValueError)

vlvl = config.get(key="verbose_lvl")
if not vlvl in ["error", "warning", "info", "debug"]:
    log.error("Harp setting verbose_lvl must be one of [error, warning, info, debug]", e=ValueError)
    
if vlvl == "error":     log.silence(harp, log.lvl.WARNING) 
if vlvl == "warning":   log.silence(harp, log.lvl.INFO) 
if vlvl == "info":      log.silence(harp, log.lvl.DEBUG) 
if vlvl == "debug":     pass