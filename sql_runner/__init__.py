import enum

class ExecutionType(str, enum.Enum):
    none="none"
    execute="execute"
    staging="staging"
    test="test"
