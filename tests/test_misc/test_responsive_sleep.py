"""Test responsive_sleep"""

import threading

from gwproactor.sync_thread import responsive_sleep
from gwproactor_test import StopWatch


class StopMe:
    def __init__(self, running: bool = True, step_duration: float = 0.1) -> None:
        self.running = running
        self.step_duration = step_duration
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def loop(self) -> None:
        while self.running:
            responsive_sleep(
                self,
                1.0,
                step_duration=self.step_duration,
                running_field_name="running",
            )

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.running = False
        self.thread.join()


MAX_DELAY = 0.01


# "pre-compile" code before tests to cut down on failures caused by
# compilation delay
def check_responsive_sleep(*, precompile_only: bool = False) -> None:
    sw = StopWatch()
    seconds = 0.1
    with sw:
        responsive_sleep(StopMe(), seconds, running_field_name="running")
    if not precompile_only:
        assert seconds <= sw.elapsed < seconds + MAX_DELAY
    seconds = 0.01
    with sw:
        responsive_sleep(StopMe(), seconds, running_field_name="running")
    if not precompile_only:
        assert seconds <= sw.elapsed < seconds + MAX_DELAY
    with sw:
        responsive_sleep(StopMe(running=False), seconds, running_field_name="running")
    if not precompile_only:
        assert 0 <= sw.elapsed < MAX_DELAY
    step_duration = 0.1
    stop_me = StopMe(step_duration=step_duration)
    stop_me.start()
    with sw:
        stop_me.stop()
    if not precompile_only:
        assert 0 <= sw.elapsed < step_duration + MAX_DELAY


def test_responsive_sleep() -> None:
    check_responsive_sleep(precompile_only=True)
    check_responsive_sleep()
