"""Cancellable wall-clock deadlines for synchronous provider operations."""
from __future__ import annotations

import signal
import threading
import time
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")


class DiscoveryTimeout(BaseException):
    """Provider discovery exceeded its bounded lease budget.

    This deliberately bypasses ordinary ``except Exception`` provider
    handlers so a library cannot turn an expired deadline into a normal result.
    """


class TranscriptTimeout(DiscoveryTimeout):
    """A transcript request exceeded its bounded item deadline."""


class MetadataTimeout(DiscoveryTimeout):
    """A video metadata request exceeded its bounded item deadline."""


class ControlPlaneTimeout(DiscoveryTimeout):
    """A control-plane request exceeded its hard wall-clock deadline."""


def _supports_posix_alarm() -> bool:
    """Return whether this process exposes the alarm API we require."""
    return all(
        hasattr(signal, name)
        for name in ("SIGALRM", "ITIMER_REAL", "setitimer", "getitimer")
    )


def _remaining_timer(previous: tuple[float, float], elapsed: float) -> tuple[float, float]:
    """Account for time spent in the operation before restoring an old timer."""
    remaining, interval = previous
    if remaining <= 0:
        return 0.0, interval
    remaining -= max(0.0, elapsed)
    if remaining > 0:
        return remaining, interval
    if interval > 0:
        # Preserve a periodic timer's next firing after one or more elapsed
        # periods. A tiny positive value lets the restored handler receive an
        # already-due signal without treating zero as "disarmed".
        remaining += (int((-remaining) // interval) + 1) * interval
    else:
        remaining = 1e-6
    return max(remaining, 1e-6), interval


def run_with_deadline(
    operation: Callable[[], T],
    timeout_seconds: float,
    *,
    timeout_type: type[DiscoveryTimeout] = DiscoveryTimeout,
) -> T:
    """Run a synchronous operation with a Unix alarm that unwinds it in place.

    A thread timeout would leave the provider operation running in the
    background. The worker is a foreground one-shot process, so SIGALRM is
    used and non-main-thread callers fail closed before starting the operation.
    """
    if (
        timeout_seconds <= 0
        or threading.current_thread() is not threading.main_thread()
        or not _supports_posix_alarm()
    ):
        raise timeout_type("provider deadline is unavailable")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, 0)
    started = time.monotonic()
    installed = False

    def alarm_handler(signum: int, frame: object) -> None:
        raise timeout_type("provider operation exceeded its lease budget")

    try:
        try:
            signal.signal(signal.SIGALRM, alarm_handler)
            installed = True
            signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
        except (OSError, ValueError) as exc:
            raise timeout_type("provider deadline is unavailable") from exc
        return operation()
    finally:
        elapsed = time.monotonic() - started
        # Disarm while our handler is still installed. Restoring the previous
        # handler first creates a race where our alarm can hit unrelated code.
        try:
            signal.setitimer(signal.ITIMER_REAL, 0)
        finally:
            if installed:
                signal.signal(signal.SIGALRM, previous_handler)
            signal.setitimer(
                signal.ITIMER_REAL,
                *_remaining_timer(previous_timer, elapsed),
            )
