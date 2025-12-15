import time
import threading


class CircuitBreakerOpen(Exception):
    pass


class CircuitBreaker:
    def __init__(
            self,
            failure_threshold: int = 5,
            reset_timeout: int = 30,
            half_open_max_calls: int = 1,
            fallback=None,
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_max_calls = half_open_max_calls
        self.fallback = fallback

        self.failures = 0
        self.state = "CLOSED"
        self.open_until = 0.0

        self._half_open_in_flight = 0
        self._lock = threading.Lock()

    def _transition_to_open(self):
        self.state = "OPEN"
        self.open_until = time.time() + self.reset_timeout
        self._half_open_in_flight = 0

    def _transition_to_half_open_if_ready(self):
        if self.state == "OPEN" and time.time() >= self.open_until:
            self.state = "HALF_OPEN"
            self._half_open_in_flight = 0

    def _try_acquire_permission(self):
        """
        Decide se la chiamata può passare.
        Ritorna (allowed: bool, state_snapshot: str, half_open_slot_taken: bool)
        """
        with self._lock:
            self._transition_to_half_open_if_ready()

            if self.state == "OPEN":
                return False, "OPEN", False

            if self.state == "HALF_OPEN":
                if self._half_open_in_flight >= self.half_open_max_calls:
                    return False, "HALF_OPEN", False
                self._half_open_in_flight += 1
                return True, "HALF_OPEN", True

            return True, "CLOSED", False

    def _release_half_open_slot(self):
        with self._lock:
            if self._half_open_in_flight > 0:
                self._half_open_in_flight -= 1

    def call(self, fn, *args, **kwargs):
        allowed, state_snapshot, took_slot = self._try_acquire_permission()

        if not allowed:
            if self.fallback is not None:
                return self.fallback(*args, **kwargs)
            raise CircuitBreakerOpen(f"Circuit breaker {state_snapshot}")

        try:
            result = fn(*args, **kwargs)
        except Exception:
            with self._lock:
                if self.state == "HALF_OPEN":
                    self._transition_to_open()
                else:
                    self.failures += 1
                    if self.failures >= self.failure_threshold:
                        self._transition_to_open()
            raise
        else:
            with self._lock:
                self.failures = 0
                self.state = "CLOSED"
                self.open_until = 0.0
                self._half_open_in_flight = 0
            return result
        finally:
            if took_slot:
                self._release_half_open_slot()
