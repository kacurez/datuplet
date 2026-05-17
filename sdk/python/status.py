"""Exit helpers for Datuplet components.

Provides structured status reporting via the DUPLET_STATUS_MESSAGE protocol.
Exit codes: 0 = success, 1 = user error, >= 20 = application error.
"""

import sys

STATUS_MESSAGE_PREFIX = "DUPLET_STATUS_MESSAGE:"


def status_message(message: str) -> None:
    """Print a status message to stdout using the DUPLET_STATUS_MESSAGE protocol.

    The K8s controller extracts this message and stores it in the CRD status.
    Call this before exiting to report a summary (e.g., "extracted 100 rows from data.csv").
    """
    print(f"{STATUS_MESSAGE_PREFIX}{message}")


def exit_user_error(message: str) -> None:
    """Print a status message and exit with code 1 (FailedUser).

    Use for user-caused errors: bad config, invalid input, schema mismatch.
    """
    status_message(message)
    sys.exit(1)


def exit_app_error(message: str) -> None:
    """Print a status message and exit with code 20 (FailedApplication).

    Use for infrastructure/application errors: connection failures, OOM, internal bugs.
    """
    status_message(message)
    sys.exit(20)
