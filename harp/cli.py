# standard library imports
from pathlib import Path
import argparse
import sys
        
# third party imports
from core.output import disp, error, rgb
        
# sub package imports
from harp.nomenclature import Nomenclature


def entry(args=None):

    # Command line arguments
    parser = argparse.ArgumentParser(description='Harmonized Ancillary Resource Provider')

    # create command
    subs = parser.add_subparsers(dest="cmd", required=True)
    
    cmd_create = subs.add_parser(
        help="Copy the default nomenclature table to the desired path (default is current working directory)",
        name='copy-table',
        )
    cmd_create.add_argument('destination',  action="store", help="Destination for the nomenclature file copy", nargs='?')
    args = parser.parse_args()

    try: # Better keyboard interupt

        if args.cmd == "copy-table":
            folder_path = Path(args.destination) if args.destination is not None else Path.cwd()
                
            if not folder_path.is_dir():
                error("Please provide an existing directory as the table copy destination")
            
            Nomenclature.copy_nomenclature_csv(target_dir=folder_path)
            
            disp(rgb.blue, f"Copied Harp Table to new file ", rgb.orange, f"{folder_path}/nomenclature.csv")
            
            # DO THINGS
    except KeyboardInterrupt:
        print()
        print("> Aborting.",  rgb.default) # restore default color
        sys.exit()
