import time
from harp._search import fuzzy, metadatastore, search_cfg
from harp._search.ascii_table import ui_styles, ascii_table
from core import log

def search(keywords, sources):

    log.disp(log.rgb.purple, "> Searching Metadatabase..", flush=True)
    
    res = []
    for df in metadatastore.get_tables():
        res.append(fuzzy.filter_best(fuzzy.search(keywords, df, source_column="search")))

    res = fuzzy.compile(res, sources)
    if len(res) ==0:
        log.disp(log.rgb.red, "\n> No match found.")
        exit()
    t = ascii_table(res, style=search_cfg.ascii_style)

    
    t.print(search_cfg.live_print)

        
    
    