class FlowForgeError(Exception):
    """Base error for anything the engine's API rejected."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class WorkflowNotFound(FlowForgeError):
    pass


class RunNotFound(FlowForgeError):
    pass


class RunAlreadyTerminal(FlowForgeError):
    pass


class LeaseLost(FlowForgeError):
    """The step's lease no longer belongs to this worker - it was
    reclaimed and possibly already picked up by someone else. Callers
    should drop the task rather than treat this as a hard failure."""
