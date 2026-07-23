"""db-unload driver — out of scope for Phase 1 (LLD §1).

The driver contract (LLD §7.6) is written so this slots in later behind the
same AcquireFn shape as s3_push/sftp_pull.
"""

from typing import NoReturn


def acquire(feed: object, window: object, fx: object) -> NoReturn:
    raise NotImplementedError("db-unload driver is not implemented in Phase 1 (LLD §1)")
