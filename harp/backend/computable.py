from typing import Callable

from core.static import interface

class Computable:
    
    @interface
    def __init__(self, func: Callable, operands=list[str], keep_operands=False):
        self.func = func
        self.operands = operands
        self.keep_operands = keep_operands