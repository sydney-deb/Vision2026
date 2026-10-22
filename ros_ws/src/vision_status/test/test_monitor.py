from vision_status.monitor import StreamMonitor


def test_waiting_ok_timeout_recovery():
    monitor = StreamMonitor(timeout=3.0)
    assert monitor.status(0) == 'WAITING'
    monitor.record(1)
    assert monitor.status(2) == 'OK'
    assert monitor.status(4.1) == 'NO DATA'
    monitor.record(5)
    assert monitor.status(5) == 'OK'


def test_rate_and_expiration():
    monitor = StreamMonitor(timeout=3, window=4)
    for t in (0, 0.5, 1, 1.5):
        monitor.record(t)
    assert abs(monitor.rate(1.5) - 2.0) < 1e-8
    assert monitor.rate(8) == 0
    assert monitor.status(8) == 'NO DATA'


def test_independent_timeout():
    camera = StreamMonitor(timeout=3)
    cloud = StreamMonitor(timeout=15)
    camera.record(1)
    cloud.record(1)
    assert camera.status(8) == 'NO DATA'
    assert cloud.status(8) == 'OK'

def test_timestamp_forward():
    monitor = StreamMonitor()

    monitor.record_stamp(10.0)
    monitor.record_stamp(10.2)
    monitor.record_stamp(10.4)

    assert monitor.backward_count == 0
    assert monitor.last_stamp == 10.4
    assert monitor.last_stamp_went_backward is False


def test_timestamp_backward():
    monitor = StreamMonitor()

    monitor.record_stamp(10.0)
    monitor.record_stamp(10.5)
    monitor.record_stamp(10.2)

    assert monitor.backward_count == 1
    assert monitor.last_stamp == 10.2
    assert monitor.last_stamp_went_backward is True


def test_timestamp_recovery():
    monitor = StreamMonitor()

    monitor.record_stamp(10.0)
    monitor.record_stamp(9.5)
    monitor.record_stamp(10.5)

    assert monitor.backward_count == 1
    assert monitor.last_stamp_went_backward is False