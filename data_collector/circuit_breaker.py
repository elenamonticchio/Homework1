import time
import threading


class CircuitBreakerOpenException(Exception):
    """Raised when the circuit breaker is open (or half-open is saturated)."""
    pass


class CircuitBreaker:
    def __init__(
            self,
            failure_threshold: int = 5,
            recovery_timeout: int = 30,
            expected_exception=Exception,
            half_open_max_calls: int = 1,
            fallback=None,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.half_open_max_calls = half_open_max_calls
        self.fallback = fallback

        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"

        self._half_open_in_flight = 0
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        with self.lock:
            if self.state == "OPEN":
                if self.last_failure_time is not None:
                    elapsed = time.time() - self.last_failure_time
                else:
                    elapsed = 0

                if elapsed >= self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    self._half_open_in_flight = 0
                else:
                    if self.fallback is not None:
                        return self.fallback(*args, **kwargs)
                    raise CircuitBreakerOpenException("Circuit is OPEN. Call denied.")

            if self.state == "HALF_OPEN":
                if self._half_open_in_flight >= self.half_open_max_calls:
                    if self.fallback is not None:
                        return self.fallback(*args, **kwargs)
                    raise CircuitBreakerOpenException("Circuit is HALF_OPEN (no slots). Call denied.")
                self._half_open_in_flight += 1
                took_slot = True
            else:
                took_slot = False

        try:
            result = func(*args, **kwargs)
        except self.expected_exception:
            with self.lock:
                self.failure_count += 1
                self.last_failure_time = time.time()

                if self.state == "HALF_OPEN":
                    self.state = "OPEN"
                    self._half_open_in_flight = 0
                elif self.failure_count >= self.failure_threshold:
                    self.state = "OPEN"
            raise
        else:
            with self.lock:
                if self.state == "HALF_OPEN":
                    self.state = "CLOSED"
                    self.failure_count = 0
                    self.last_failure_time = None
                    self._half_open_in_flight = 0
                else:
                    self.failure_count = 0
            return result

        finally:
            if took_slot:
                with self.lock:
                    if self._half_open_in_flight > 0:
                        self._half_open_in_flight -= 1
