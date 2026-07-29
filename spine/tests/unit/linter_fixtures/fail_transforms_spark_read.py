"""MUST-FAIL: a transform reaching for the Spark reader directly (`.read(...)`)
instead of taking its input as a declared, framework-supplied argument — LLD
004.1 §12.3's `frames-transforms` profile bans the `read` attribute-call by
name alone, regardless of receiver (D-9: transforms/`frames/` never do I/O).

Simulated scope: `spine/frames/**` or a pipeline's `transforms.py`
(`frames-transforms` profile applies).
"""

from __future__ import annotations


def apply(source):
    return source.read()
