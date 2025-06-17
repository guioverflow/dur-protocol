
from enum import Enum

class Operation(Enum):
    READ = "READ"
    WRITE = "WRITE"
    ABORT = "ABORT"
    COMMIT = "COMMIT"