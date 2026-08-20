from dataclasses import dataclass, field
from typing import Callable, Optional

from .retry import RetryPolicy


@dataclass
class StepSpec:
    name: str
    fn: Callable
    timeout_seconds: int = 30
    delay_seconds: int = 0
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    compensation_of: str = ""

    def to_dict(self):
        d = {
            "name": self.name,
            "timeout_seconds": self.timeout_seconds,
            "delay_seconds": self.delay_seconds,
            "retry": self.retry.to_dict(),
        }
        if self.compensation_of:
            d["compensation_of"] = self.compensation_of
        return d


class Workflow:
    """Defines a workflow as an ordered sequence of steps. Steps are
    registered in the order their @workflow.step decorators run, which
    is the order the engine will execute them in - there's no separate
    place to declare ordering.
    """

    def __init__(self, name: str, version: int = 1, max_concurrent_runs: int = 0):
        self.name = name
        self.version = version
        self.max_concurrent_runs = max_concurrent_runs
        self.steps: list[StepSpec] = []
        self.compensations: list[StepSpec] = []

    def step(
        self,
        name: str,
        *,
        timeout_seconds: int = 30,
        delay_seconds: int = 0,
        retry: Optional[RetryPolicy] = None,
    ):
        def decorator(fn):
            self.steps.append(
                StepSpec(
                    name=name,
                    fn=fn,
                    timeout_seconds=timeout_seconds,
                    delay_seconds=delay_seconds,
                    retry=retry or RetryPolicy(),
                )
            )
            return fn

        return decorator

    def compensate(
        self,
        step_name: str,
        *,
        name: Optional[str] = None,
        timeout_seconds: int = 30,
        retry: Optional[RetryPolicy] = None,
    ):
        def decorator(fn):
            self.compensations.append(
                StepSpec(
                    name=name or f"compensate_{step_name}",
                    fn=fn,
                    timeout_seconds=timeout_seconds,
                    retry=retry or RetryPolicy(),
                    compensation_of=step_name,
                )
            )
            return fn

        return decorator

    def handler_for(self, step_name: str, is_compensation: bool) -> Optional[StepSpec]:
        pool = self.compensations if is_compensation else self.steps
        for spec in pool:
            if spec.name == step_name:
                return spec
        return None

    def to_definition(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "max_concurrent_runs": self.max_concurrent_runs,
            "steps": [s.to_dict() for s in self.steps],
            "compensations": [s.to_dict() for s in self.compensations],
        }
