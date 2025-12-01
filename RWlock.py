import threading
from threading import RLock

class ReadWriteLock:
    def __init__(self):
        self._read_ready = threading.Condition(RLock())
        self._readers = 0
        self._writers = 0
        self._write_waiters = 0
    
    def acquire_read(self):
        self._read_ready.acquire()
        while self._writers > 0 or self._write_waiters > 0:
            self._read_ready.wait()
        self._readers += 1
        self._read_ready.release()
    
    def release_read(self):
        self._read_ready.acquire()
        self._readers -= 1
        if self._readers == 0:
            self._read_ready.notify_all()
        self._read_ready.release()
    
    def acquire_write(self):
        self._read_ready.acquire()
        self._write_waiters += 1
        while self._readers > 0 or self._writers > 0:
            self._read_ready.wait()
        self._write_waiters -= 1
        self._writers += 1
        self._read_ready.release()
    
    def release_write(self):
        self._read_ready.acquire()
        self._writers -= 1
        self._read_ready.notify_all()
        self._read_ready.release()