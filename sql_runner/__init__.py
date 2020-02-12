class ExecutionType:
    none: "ExecutionType" = ""
    execute: "ExecutionType" = ""
    staging: "ExecutionType" = ""
    test: "ExecutionType" = ""

    def __init__(self, execution_type: str):
        self.execution_type=execution_type

    def __str__(self):
        return self.execution_type

    def __repr__(self):
        return f"<ExecutionType(\"{self.execution_type}\")>"


ExecutionType.none = ExecutionType("none")
ExecutionType.execute = ExecutionType("execute")
ExecutionType.staging = ExecutionType("execute")
ExecutionType.test = ExecutionType("test")
