"""
Error Recovery Engine
Retry logic, dead-letter queue, and circuit breaker for the ETL pipeline.
Nothing gets silently dropped — every failure is tracked and recoverable.
"""

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Optional
from enum import Enum


class FailureReason(str, Enum):
    TRANSFORM_ERROR    = "TRANSFORM_ERROR"
    VALIDATION_ERROR   = "VALIDATION_ERROR"
    DB_CONNECTION      = "DB_CONNECTION"
    DB_CONSTRAINT      = "DB_CONSTRAINT"
    NETWORK_TIMEOUT    = "NETWORK_TIMEOUT"
    GRAPH_API_ERROR    = "GRAPH_API_ERROR"
    UNKNOWN            = "UNKNOWN"


class CircuitState(str, Enum):
    CLOSED   = "CLOSED"    # Normal — requests pass through
    OPEN     = "OPEN"      # Tripped — requests blocked
    HALF_OPEN = "HALF_OPEN" # Testing — one request allowed


@dataclass
class DeadLetterItem:
    """A record that failed all retries and sits in the dead-letter queue."""
    dlq_id:         str
    item_id:        str
    resource_id:    str
    resource_type:  str
    payload:        dict[str, Any]
    failure_reason: FailureReason
    error_message:  str
    retry_count:    int
    first_failed_at: datetime
    last_failed_at:  datetime = field(default_factory=datetime.utcnow)
    resolved:        bool = False
    resolved_at:     Optional[datetime] = None


@dataclass
class RetryRecord:
    item_id:      str
    attempt:      int
    error:        str
    attempted_at: datetime = field(default_factory=datetime.utcnow)


class CircuitBreaker:
    """
    Prevents hammering a failing downstream system.
    After N failures, opens the circuit and blocks calls for a cooldown period.
    """

    def __init__(self, failure_threshold: int = 5, cooldown_secs: int = 60):
        self._threshold  = failure_threshold
        self._cooldown   = cooldown_secs
        self._failures   = 0
        self._state      = CircuitState.CLOSED
        self._opened_at: Optional[datetime] = None

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if self._opened_at and (datetime.utcnow() - self._opened_at).seconds >= self._cooldown:
                self._state = CircuitState.HALF_OPEN
        return self._state

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        if self.state == CircuitState.OPEN:
            raise RuntimeError(f"Circuit OPEN — cooldown {self._cooldown}s after {self._failures} failures")

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        self._failures = 0
        self._state    = CircuitState.CLOSED

    def _on_failure(self):
        self._failures += 1
        if self._failures >= self._threshold:
            self._state    = CircuitState.OPEN
            self._opened_at = datetime.utcnow()

    def get_status(self) -> dict:
        return {
            "state":    self.state.value,
            "failures": self._failures,
            "threshold":self._threshold,
        }


class ErrorRecoveryEngine:
    """
    Wraps ETL operations with retry logic and dead-letter queuing.
    Any item that fails all retries lands in the DLQ for manual review or replay.
    """

    DEFAULT_MAX_RETRIES  = 3
    DEFAULT_BACKOFF_SECS = [1, 5, 15]  # Exponential-ish backoff

    def __init__(self, max_retries: int = 3):
        self._max_retries  = max_retries
        self._dlq:         list[DeadLetterItem]  = []
        self._retry_log:   list[RetryRecord]     = []
        self._circuit      = CircuitBreaker()
        self._dlq_counter  = 0
        self._success_count = 0
        self._failure_count = 0

    def execute_with_retry(
        self,
        fn:           Callable,
        item_id:      str,
        resource_id:  str,
        resource_type:str,
        payload:      dict,
        *args,
        **kwargs,
    ) -> tuple[bool, Any]:
        """
        Execute fn with retry + circuit breaker.
        Returns (success, result_or_error).
        Sends to DLQ after all retries exhausted.
        """
        last_error = None
        backoffs   = self.DEFAULT_BACKOFF_SECS + [30] * self._max_retries

        for attempt in range(self._max_retries + 1):
            try:
                result = self._circuit.call(fn, *args, **kwargs)
                self._success_count += 1
                return True, result

            except RuntimeError as e:
                # Circuit open — send straight to DLQ
                last_error = str(e)
                self._retry_log.append(RetryRecord(item_id=item_id, attempt=attempt, error=last_error))
                break

            except Exception as e:
                last_error = str(e)
                self._retry_log.append(RetryRecord(item_id=item_id, attempt=attempt + 1, error=last_error))

                if attempt < self._max_retries:
                    wait = backoffs[min(attempt, len(backoffs) - 1)]
                    time.sleep(wait * 0.01)  # Scale down for demo speed
                    continue
                break

        # All retries exhausted → DLQ
        self._failure_count += 1
        self._send_to_dlq(item_id, resource_id, resource_type, payload, last_error)
        return False, last_error

    def _send_to_dlq(
        self,
        item_id:      str,
        resource_id:  str,
        resource_type:str,
        payload:      dict,
        error:        str,
    ):
        self._dlq_counter += 1
        reason = self._classify_error(error)
        self._dlq.append(DeadLetterItem(
            dlq_id=f"DLQ-{self._dlq_counter:04d}",
            item_id=item_id,
            resource_id=resource_id,
            resource_type=resource_type,
            payload=payload,
            failure_reason=reason,
            error_message=error or "Unknown error",
            retry_count=self._max_retries,
            first_failed_at=datetime.utcnow(),
        ))

    def replay_dlq(self, fn: Callable, filter_reason: FailureReason = None) -> dict:
        """Replay items from the dead-letter queue."""
        items = [d for d in self._dlq if not d.resolved]
        if filter_reason:
            items = [d for d in items if d.failure_reason == filter_reason]

        replayed = success = failed = 0
        for item in items:
            replayed += 1
            try:
                fn(item.payload)
                item.resolved    = True
                item.resolved_at = datetime.utcnow()
                success += 1
            except Exception:
                item.retry_count += 1
                item.last_failed_at = datetime.utcnow()
                failed += 1

        return {"replayed": replayed, "success": success, "failed": failed}

    def _classify_error(self, error: str) -> FailureReason:
        if not error:
            return FailureReason.UNKNOWN
        e = error.lower()
        if "timeout" in e or "timed out" in e:   return FailureReason.NETWORK_TIMEOUT
        if "connection" in e:                     return FailureReason.DB_CONNECTION
        if "constraint" in e or "unique" in e:   return FailureReason.DB_CONSTRAINT
        if "graph" in e or "401" in e:            return FailureReason.GRAPH_API_ERROR
        if "transform" in e:                      return FailureReason.TRANSFORM_ERROR
        if "validat" in e:                        return FailureReason.VALIDATION_ERROR
        return FailureReason.UNKNOWN

    def get_dlq(self, resolved: bool = False) -> list[DeadLetterItem]:
        return [d for d in self._dlq if d.resolved == resolved]

    def get_circuit_status(self) -> dict:
        return self._circuit.get_status()

    def get_stats(self) -> dict:
        by_reason: dict[str, int] = {}
        for item in self._dlq:
            key = item.failure_reason.value
            by_reason[key] = by_reason.get(key, 0) + 1
        return {
            "success_count":  self._success_count,
            "failure_count":  self._failure_count,
            "dlq_pending":    len([d for d in self._dlq if not d.resolved]),
            "dlq_resolved":   len([d for d in self._dlq if d.resolved]),
            "dlq_by_reason":  by_reason,
            "circuit":        self._circuit.get_status(),
            "retry_attempts": len(self._retry_log),
        }
