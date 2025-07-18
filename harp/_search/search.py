import time
from harp._search import metadatastore, search_cfg, search_engine
from harp._search.ascii_table import ui_styles, ascii_table
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
        t = ascii_table(res, style=search_cfg.ascii_style)

        
        t.print(search_cfg.live_print)
