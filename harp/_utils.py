class Record:
    """
    Dynamic structure, in between a dict and a fully fledge class
    Acts as a dynamic data class
    Mostly used to pass and returns values cleany and explicitely instead of using unamed tuples for example.
    
    
    return ("John", 10001, False) 
    vs
    return Record(name="John", id=10001, admin=False)
     
    """
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
            
    def __repr__(self):
        return f"{str(self.__dict__)}"
        
    def keys(self):
        return self.__dict__.keys()
        
    def items(self):
        return self.__dict__.items()
    
            