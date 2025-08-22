from core import log
from harp._search import search_cfg
from harp._search.search import search
    
import harp

import argparse
from sys import exit

def entry(args=None):
    
    # Command line arguments
    parser = argparse.ArgumentParser(description="HARP search engine CLI tool", 
        epilog="Made by HYGEOS."
    )

    subs = parser.add_subparsers(dest="command", required=True)
    
    # > search command
    cmd = subs.add_parser(help="search variables in the datasets interfaced by HARP", name="search")
    cmd.add_argument(
        "keywords", 
        nargs="+",  # Captures ALL remaining arguments
        help="Keywords to search for in the database"
    )
    
    cmd.add_argument("--debug", action="store_true", help="Debug mode (developper)", default=False)
    # cmd.add_argument("--show-query-name", "-q", action="store_true", help="Show the query namne column", default=False)
    cmd.add_argument("--minimum", "--min", action="store", help="Minimum match score to consider [20-100]", 
        default=None, metavar="match_threshold"
    )
    
    cmd.add_argument("--from", action="store", help="Dataset source selection (Like NASA, ERA5 etc..)", 
        default=None, nargs="+", metavar="source"
    )
    
    # Create a mutually exclusive group
    mode_group = cmd.add_mutually_exclusive_group()
    mode_group.add_argument("--exact", "-e",        action="store_true", help="Exact matching")
    mode_group.add_argument("--strict", "-s",       action="store_true", help="Strict matching")
    # mode_group.add_argument("--approximate", "-a",  action="store_true", help="Approximate matching")

    width_group = cmd.add_mutually_exclusive_group()
    width_group.add_argument("--large", "-l", action="store_true", help="Display columns to their max width", default=False)
    
    cmd.add_argument("--nocolor", "-n", action="store_true", help="Disable color output", default=False)
    cmd.add_argument("--compact", "-c", action="store_true", help="Compact layout", default=False)
    
    
    cmd.add_argument(
        "--style", type=str,
        choices=["simple", "rounded", "square", "double"],
        help="Select table style (simple, rounded, square)",
        default="squared"  # Or whatever default you prefer
    )
    
    
    args = parser.parse_args()
    
    if args.minimum is not None: 
        minimum = str(args.minimum).replace("%", " ")
        minimum = int(minimum)
        if minimum < 0 or minimum > 100:
            print("Expected range for --minimum param: [20-100]")    
            exit()
        if 0 <= minimum < 20:
            minimum = 20
            
        search_cfg.match_threshold = minimum
        search_cfg.user_match_treshold = True
    
    
    search_cfg.display_query_name = False # args.show_query_name
    
    if args.exact: search_cfg.match_exact = True
    if args.strict: search_cfg.match_strict = True
    # if args.approximate: search_cfg.match_approx = True
    if args.debug: search_cfg.debug = True
    if args.nocolor: 
        search_cfg.ascii_nocolor = True
        log.config.show_color = False
    
    search_cfg.compact = args.compact
    search_cfg.large = args.large
    
    
    sources = getattr(args, "from")
    if sources and not isinstance(sources, list):
        sources = [sources]
        
    search_cfg.ascii_style = args.style
        
    if not args.debug:
        log.silence(harp, log.lvl.DEBUG)
        
        
    apply_user_search_config()
    
    search(args.keywords, sources=sources)


def apply_user_search_config():
    
    buf_word_threshold = search_cfg.word_threshold
    buf_match_threshold = search_cfg.match_threshold
    
    # --strict and --approx modes
    if search_cfg.match_exact: search_cfg.word_threshold += 0.20
    if search_cfg.match_strict: search_cfg.word_threshold += 0.10
    if search_cfg.match_approx: search_cfg.word_threshold -= 0.05
    
    if not search_cfg.user_match_treshold:
        if search_cfg.match_exact:  search_cfg.match_threshold += 30
        if search_cfg.match_strict: search_cfg.match_threshold += 20
        if search_cfg.match_approx: search_cfg.match_threshold -= 30
    
    if search_cfg.debug:
        log.disp(log.rgb.orange, "DEBUG:")
        log.disp(log.rgb.orange, f"  match_strict: {search_cfg.match_strict}")
        log.disp(log.rgb.orange, f"  match_approx: {search_cfg.match_approx}")
        log.disp(log.rgb.orange, f"  word_threshold: {buf_word_threshold} -> {search_cfg.word_threshold}")
        log.disp(log.rgb.orange, f"  match_threshold: {buf_match_threshold} -> {search_cfg.match_threshold}")
        
        # log.disp(log.rgb.orange, f"changed: word_threshold -> {search_cfg.word_threshold}")
