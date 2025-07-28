from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep

from core import log

class ComputeLock:
    
    def __init__(self, filepath: Path, timeout: int = -1, lifetime: timedelta|None = None, interval: int = 1):
        """
        Initialize the object with file path, lock timeout, and lock lifetime.

        Args:
        filepath (Path): Path to the file.
        lock_timeout (int): Maximum time to wait for a lock (in seconds), error afterward. (-1 for infinite wait)
        lock_lifetime (timedelta): Duration a lock remains valid.
        interval (int): Duration to wait before checking lockfile again (in seconds).
        
        """
        
        self.filepath = Path(filepath)
        self.timeout  = timeout if timeout >= 0 else timedelta(days=99999).total_seconds() # defaults to many years
        self.lifetime = lifetime or timedelta(days=99999) # defaults to many years
        self.interval = interval
        
        
    def is_free(self):
        return not self.filepath.is_file()
    
    
    def is_locked(self):
        # add lock staleness mgmt
        return self.filepath.is_file()
        
    
    # def acquire(self):
    #     # create lockfile
    #     ...
    
        
    # def release(self):
    #     # delete lockfile
    #     ...

    
    def wait(self):
        log.debug(f"Waiting for lockfile '{self.filepath}' to be cleared..")
        
        # wait for underlying lockfile to be cleared
        
        start = datetime.now()        
        while self.filepath.exists():
            
            delta = datetime.now() - start
            
            if delta.total_seconds() > self.timeout:
                raise TimeoutError(f'Timeout on Lockfile "{self.filepath}"')
                
            sleep(self.interval)
    
    
    def locked(self): # context manager
        
        
        @contextmanager
        def _lock_routine():
        
            if self.is_locked():
                self.wait()
            
            self.filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # create the lock file
            with open(self.filepath, 'w') as fd:
                fd.write('')

            try: # yield context manager
                yield self.filepath
                
            finally: # remove the lock file 
                self.filepath.unlink()
        
        return _lock_routine()
    
    
    def _manage_staleness(self):
        
        if self.is_free(): return
        
        lockfile_age = datetime.now() - datetime.fromtimestamp(self.filepath.stat().st_ctime)
        if lockfile_age > self.config.get("lock_lifetime"):
            log.debug(f"Removing lockfile {self.filepath}, considered stale. ")
            self.filepath.unlink()
    