import pytest

from dmanager.speedcalculator import SpeedCalculator


def test_initial_speed_is_zero():
    sc = SpeedCalculator()
    assert sc.get_speed() == 0.0

def test_add_bytes_and_speed_basic(monkeypatch):
    # Patch time.monotonic to control time
    fake_time = [1000.0]
    def fake_monotonic():
        return fake_time[0]

    monkeypatch.setattr("time.monotonic", fake_monotonic)

    sc = SpeedCalculator(window_sec=2.0)
    sc.add_bytes(100)
    # Only one sample → speed = 0
    assert sc.get_speed() == 0.0

    fake_time[0] += 1.0
    sc.add_bytes(200)

    # total bytes = 100 + 200 = 300, delta time = 1.0
    assert pytest.approx(sc.get_speed(), 0.01) == 300.0

def test_window_removes_old_samples(monkeypatch):
    fake_time = [1000.0]
    def fake_monotonic():
        return fake_time[0]

    monkeypatch.setattr("time.monotonic", fake_monotonic)

    sc = SpeedCalculator(window_sec=2.0)
    sc.add_bytes(100)
    fake_time[0] += 1.0
    sc.add_bytes(200)
    fake_time[0] += 2.0  # first sample expires
    sc.add_bytes(50)

    # Only the last two samples count: 200 + 50
    expected_speed = (200 + 50) / 2
    assert pytest.approx(sc.get_speed(), 0.01) == expected_speed

def test_short_interval_returns_zero(monkeypatch):
    fake_time = [1000.0]
    def fake_monotonic():
        return fake_time[0]

    monkeypatch.setattr("time.monotonic", fake_monotonic)

    sc = SpeedCalculator()
    sc.add_bytes(100)
    fake_time[0] += 0.1
    sc.add_bytes(200)

    # delta_time < 0.15 → unstable → returns 0
    assert sc.get_speed() == 0.0

def test_multiple_samples(monkeypatch):
    fake_time = [0.0]
    def fake_monotonic():
        return fake_time[0]

    monkeypatch.setattr("time.monotonic", fake_monotonic)

    sc = SpeedCalculator(window_sec=5.0)
    sc.add_bytes(100)
    fake_time[0] += 1.0
    sc.add_bytes(200)
    fake_time[0] += 1.0
    sc.add_bytes(300)
    fake_time[0] += 1.0

    # total bytes = 100+200+300=600, delta_time=2.0
    assert pytest.approx(sc.get_speed(), 0.01) == 600 / 2.0
