import pytest

from app.main import CircuitBreaker
from datetime import timedelta


def test_circuit_breaker_transitions_on_failures():
    breaker = CircuitBreaker(failure_threshold=2, timeout=1)

    with pytest.raises(ValueError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("boom")))

    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("crash")))

    assert breaker.state == "OPEN"

    with pytest.raises(Exception, match="Circuit breaker is OPEN"):
        breaker.call(lambda: "should not run")


def test_circuit_breaker_recovers_after_timeout(monkeypatch):
    breaker = CircuitBreaker(failure_threshold=1, timeout=0.1)

    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("fail")))

    assert breaker.state == "OPEN"

    # Fast-forward time by manipulating last_failure_time
    breaker.last_failure_time -= timedelta(seconds=breaker.timeout + 0.1)

    result = breaker.call(lambda: "ok")

    assert result == "ok"
    assert breaker.state == "CLOSED"

