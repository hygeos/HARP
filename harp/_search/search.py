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
            spatial     = log.rgb.orange,
            units       = log.rgb.purple,
            name        = None,
            dataset     = log.rgb.blue,
            timerange   = log.rgb.orange,
            short_name  = None, # log.rgb.green, # log.rgb.cyan,
            query_name  = log.rgb.cyan, # log.rgb.cyan,
        )            
            
        t = ascii_table(res, style=ascii_table.style(style=search_cfg.ascii_style), colors=colors, max_width=search_cfg.ascii_max_col_chars)
        t.print(search_cfg.live_print, search_cfg.ascii_nocolor)
