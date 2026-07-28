"""MUST-FAIL: a plain class nested inside a function is still a banned class shape.

Simulated scope: any (idiom rule has no exemptions).
"""


def make_thing():
    class Inner:
        def __init__(self, x: int) -> None:
            self.x = x

    return Inner
