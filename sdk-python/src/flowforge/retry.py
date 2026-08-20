from dataclasses import dataclass


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_base_ms: int = 1000
    backoff_multiplier: float = 2.0
    backoff_max_ms: int = 60_000
    jitter_fraction: float = 0.2

    def to_dict(self):
        return {
            "max_attempts": self.max_attempts,
            "backoff_base_ms": self.backoff_base_ms,
            "backoff_multiplier": self.backoff_multiplier,
            "backoff_max_ms": self.backoff_max_ms,
            "jitter_fraction": self.jitter_fraction,
        }
