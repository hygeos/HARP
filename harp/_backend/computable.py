from typing import Callable

from core.static import interface

class Computable:
    """
    Computable Variables Utility
    
    Defines a class for representing computable variables that can be derived from
    existing datasets using provided functions and operands (variables).
    """    
    
    # @interface
    def __init__(self, func: Callable, operands: list[str], keep_operands=False):
        self.func = func
        self.operands = operands
        self.keep_operands = keep_operands