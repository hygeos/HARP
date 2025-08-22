import time
from harp._search import metadatastore, search_cfg, search_engine
from harp._search.ascii_table import ascii_table
from core import log
from core.monitor import Chrono

def search(keywords, sources):

    with Chrono("Execution time", unit="s"):
        log.info("Searching Metadatabase..", flush=True)
        
        res = []
        for df in metadatastore.get_tables():
            res.append(search_engine.filter_best(search_engine.search(keywords, df, source_column="search")))

        res = search_engine.compile(res, sources)
        if len(res) ==0:
            log.disp("> No match found.")
            exit()
            
            
        colors = dict(
            match       = log.rgb.gray,
            dims        = log.rgb.red,
            resolution  = log.rgb.orange,
            units       = log.rgb.purple,
            name        = None,
            param       = log.rgb.cyan,
            dataset     = log.rgb.blue,
            timerange   = log.rgb.orange,
            short_name  = None, # log.rgb.green, # log.rgb.cyan,
            query_name  = log.rgb.cyan, # log.rgb.cyan,
        )
        
        ovrd = 32 if search_cfg.large else 0
        default = 999
        
        widths = dict(
            match       = ovrd or default,
            dims        = ovrd or default,
            resolution  = ovrd or default,
            units       = ovrd or      10,
            name        = ovrd or      60,
            param       = ovrd or default,
            dataset     = ovrd or      25,
            timerange   = ovrd or default,
            short_name  = ovrd or default,
            query_name  = ovrd or default,
        )
        
        # modify padding depending on compact param
        s = ascii_table.style(style=search_cfg.ascii_style)
        s.h_padding = int(not search_cfg.compact)
        
        t = ascii_table(res, style=s, colors=colors, widths=widths)
        t.print(search_cfg.live_print, search_cfg.ascii_nocolor)
