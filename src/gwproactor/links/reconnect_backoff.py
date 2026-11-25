"""The wait schedule between a link's connection attempts."""

import random

MAX_RECONNECT_WAIT_S = 30.0
"""Longest wait between connection attempts. Bounds how long a link sits
idle after its broker is back."""


class ReconnectBackoff:
    """Full-jitter schedule: each wait is random in [0, step]; the step
    doubles from 1 s to the cap and stays there; an accepting CONNACK resets
    it.

    The jitter is for the broker and everything behind a login. Every
    reconnect is a TLS handshake, a FIS lease check, a session with its
    subscriptions restored and the proactor's held reports flushed; a fleet
    whose links lost the broker in the same second returns in phase without
    it, every step, to a broker that may be struggling after a restart.
    """

    max_wait_s: float
    step_s: float
    rng: random.Random

    def __init__(self, max_wait_s: float, rng: random.Random | None = None) -> None:
        self.max_wait_s = max_wait_s
        self.step_s = 1.0
        # the jitter spreads a fleet's reconnects; it protects no secret
        self.rng = rng if rng is not None else random.Random()  # noqa: S311

    def next_wait(self) -> float:
        wait = self.rng.uniform(0, self.step_s)
        self.step_s = min(self.step_s * 2, self.max_wait_s)
        return wait

    def reset(self) -> None:
        self.step_s = 1.0
