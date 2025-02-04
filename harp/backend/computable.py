from typing import Callable

from core.static import interface

class Computable:
    
    @interface
    def __init__(self, func: Callable, operands=list[str]):
        self.func = func
        self.operands = operands