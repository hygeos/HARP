import shutil
import pandas as pd

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


class ui_style(Enum): 
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
    
    def __init__(self, df: pd.DataFrame, style=ui_style.simple):
        self.df = df
        self.style = style
        
        df_str = df.astype(str)
        self.columns: list[str] = df_str.columns
        self.spaces: list[int] = df_str[self.columns].apply(
            lambda col: max(len(col.name), col.str.len().max())
        ).tolist()
        self.terminal = shutil.get_terminal_size()
        
    
    def __repr__(self):
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
    
        
        def _line_separator(pos, widths):
            """
            Generates an ascii separator line for the table based on position.
            """
            
            if pos == "mid":
                
                ivbc = cr if ivb else h       # cross if inner vertical bars, line otherwise
                sep = h*hp + ivbc + h*hp      # construct mid separator (not on the outer side)
                if not ihb: sep = h*hp        # if no vertical bar, reduce padding
                
                mids = [h*w for w in widths]
                line = ml + h*hp + sep.join(mids) + h*hp + mr
                return line
            
            if pos == "top":
                
                ivbc = mt if ivb else h
                sep = h*hp + ivbc + h*hp
                if not ihb: sep = h*hp
                
                mids = [h*w for w in widths]
                line = tl + h*hp + sep.join(mids) + h*hp + tr
                return line
            
            if pos == "bot":
                
                ivbc = mb if ivb else h
                sep = h*hp + ivbc + h*hp
                if not ihb: sep = h*hp
                
                mids = [h*w for w in widths]
                line = bl + h*hp + sep.join(mids) + h*hp + br
                return line
            
        
        def _line_padding(widths):
            """
            Generates an ascii padding line for the table.
            """

            ivbc = v if ivb else h
            sep = " "*hp + ivbc + " "*hp
            if not ihb: sep = " "*hp
        
            mids = [" "*w for w in widths]
            line = ml + " "*hp + sep.join(mids) + " "*hp + mr
            return line
        
        
        def _line_content(line, widths):
            """
            Generates an ascii content line for the table.
            """
            
            ivbc = v if ivb else h
            sep = " "*hp + ivbc + " "*hp
            if not ihb: sep = " "*hp
        
            mids = [str(w).ljust(widths[i]) for i, w in enumerate(line)]
            line = v + " "*hp + sep.join(mids) + " "*hp + v
            return line
    
    
        """
        Main routine using the previous subroutines
        Iterates over each lines of the dataframe and builds the ascii representation
        """
        
        entries = self.df[self.columns].values
        lines = []
        
        lines.append(_line_separator("top",  self.spaces))
        lines.append(_line_content(self.columns, self.spaces))
        lines.append(_line_separator("mid",  self.spaces))
        for e in entries:
            for i in range(vp): lines.append(_line_padding("mid", self.spaces))
            lines.append(_line_content(e, self.spaces))
            for i in range(vp): lines.append(_line_padding("mid", self.spaces))
            if ihb:
                lines.append(_line_separator("mid",  self.spaces))
                
        lines.append(_line_separator("bot", self.spaces))
        
        message = "\n".join(lines)
        return message