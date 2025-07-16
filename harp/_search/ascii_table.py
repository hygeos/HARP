import time
import shutil
import pandas as pd

from harp._search import search_cfg
from harp._utils import Record
from enum import Enum


class _style:
    """
    A class to define the style of a table using specific characters.
    """
    class ui_cfg:
        """
        Configuration for UI elements like bars and padding.
        """

        def __init__(self):
            self.outer_vbar = True
            self.outer_hbar = True  
            self.inner_vbar = True
            self.inner_hbar = False
            self.hpadding = 1
            self.vpadding = 0
    
    def __init__(self, style:str):
        self.h      = style[0]   # horizontal char
        self.v      = style[1]   # vertical char
        self.tl     = style[2]   # corner top left
        self.tr     = style[3]   # corner top right
        self.bl     = style[4]   # corner bot left
        self.br     = style[5]   # corner bot right
        self.ml     = style[6]   # mid left 
        self.mr     = style[7]   # mid right
        self.mt     = style[8]   # mid top
        self.mb     = style[9]   # mid bot
        self.cr     = style[10]  # cross
        
        self.cfg = _style.ui_cfg()


class ui_styles(Enum): 
    """
    Enum class for building different table styles.
    """

    squared = _style("─│┌┐└┘├┤┬┴┼")
    rounded = _style("─│╭╮╰╯├┤┬┴┼")
    simple  = _style("-|+++++++++")
    double  = _style("═║╔╗╚╝╠╣╦╩╬")


class ascii_table:
    
    """
    A class to represent a table for displaying data in a formatted way.
    """
    
    def __init__(self, df: pd.DataFrame, style=ui_styles.simple):
        self.df = df
        self.style = style.value
        
        df_str = df.astype(str)
        self.columns: list[str] = df_str.columns
        self.spaces: list[int] = df_str[self.columns].apply(
            lambda col: max(len(col.name), col.str.len().max())
        ).tolist()
        self.terminal = shutil.get_terminal_size()
        
        # limiting column max width
        max_width = search_cfg.ascii_max_col_chars
        if max_width is not None:
            for i, v in enumerate(self.spaces):
                if v > max_width:
                    self.spaces[i] = max_width
        
        
        
    def print(self, live_print=False):
        self.to_string(live_print)
        
    
    def to_string(self, live_print=False):
        """
        Generates an ascii representation of the dataset provided in the constructor.
        """
        
        # NOTE: does not (yet) handle line reduction with ellipsis
            # ellipsis_char = "‥" # "…" # two dots variants seems more readable 
            # width_footprint = sum(self.widths) + 3*len(self.widths) + 1
            # width_overshoot = width_footprint - self.terminal.columns
    
        # config
        ivb = self.style.cfg.inner_vbar
        ihb = self.style.cfg.inner_hbar
        hp  = self.style.cfg.hpadding
        vp  = self.style.cfg.vpadding
        
        # style characters
        h   = self.style.h  # horizontal
        v   = self.style.v  # vertical 
        tl  = self.style.tl # top left corner            
        tr  = self.style.tr # top right corner
        bl  = self.style.bl # bottom left
        br  = self.style.br # bottom right
        mr  = self.style.mr # mid right              
        mt  = self.style.mt # mid top         
        ml  = self.style.ml # mid left           
        mb  = self.style.mb # mid bottom         
        cr  = self.style.cr # cross
    
        # Precompute common strings
        hhp = h * hp
        shp = ' ' * hp
        h_line = h * (hp * 2 + 1)  # For cases without inner bars
        
        def _line_separator(pos, spaces):
            """
            Generates an ascii separator line for the table based on position.
            """
            
            if pos == "mid":
                
                ivbc = cr if ivb else h       # cross if inner vertical bars, line otherwise
                sep = hhp + ivbc + hhp      # construct mid separator (not on the outer side)
                if not ivb: sep = hhp        # if no vertical bar, reduce padding
                
                mids = [h*w for w in spaces]
                line = ml + hhp + sep.join(mids) + hhp + mr
                return line
            
            if pos == "top":
                
                ivbc = mt if ivb else h
                sep = hhp + ivbc + hhp
                if not ivb: sep = hhp
                
                mids = [h*w for w in spaces]
                line = tl + hhp + sep.join(mids) + hhp + tr
                return line
            
            if pos == "bot":
                
                ivbc = mb if ivb else h
                sep = hhp + ivbc + hhp
                if not ivb: sep = hhp
                
                mids = [h*w for w in spaces]
                line = bl + hhp + sep.join(mids) + hhp + br
                return line
            
        
        def _line_padding(spaces):
            """
            Generates an ascii padding line for the table.
            """

            ivbc = v if ivb else h
            sep = shp + ivbc + shp
            if not ihb: sep = shp
        
            mids = [" "*w for w in spaces]
            line = ml + shp + sep.join(mids) + shp + mr
            return line
        
        
        def _limit_str(s):
            l = search_cfg.ascii_max_col_chars
            if len(s) > l:
                return s[:l-1] + "‥"
            return s
                
        # disable for performance reason
        if not search_cfg.ascii_max_col_chars:
            _limit_str = lambda x: x
        
        def _line_content(line, spaces):
            """
            Generates an ascii content line for the table.
            """
            
            ivbc = v if ivb else h
            sep = shp + ivbc + shp
            if not ivb: sep = shp
        
            mids = [_limit_str(str(w)).ljust(spaces[i]) for i, w in enumerate(line)]
            line = v + shp + sep.join(mids) + shp + v
            return line
    
    
        """
        Main routine using the previous subroutines
        Iterates over each lines of the dataframe and builds the ascii representation
        """
        
        entries = self.df[self.columns].values
        lines = []
        
        headers = [s.capitalize() for s in self.columns]
        spaces = self.spaces.copy()
        
        
        itr = search_cfg.live_print_target_time / len(self.df)
        itr = min(itr, 0.015) 
        fn = lines.append if not live_print else lambda x: (time.sleep(itr), print(x, flush=True))
        
        fn("")
        fn(_line_separator("top",  spaces))
        fn(_line_content(headers, spaces))
        fn(_line_separator("mid",  spaces))
        for j, e in enumerate(entries):
            
            for i in range(vp): fn(_line_padding("mid", spaces))
            fn(_line_content(e, spaces))
            for i in range(vp): fn(_line_padding("mid", spaces))
            if ihb:
                fn(_line_separator("mid",  spaces))
                
        fn(_line_separator("bot", spaces))
        fn("")
        
        
        if not live_print: 
            message = "\n".join(lines)
            print(message)