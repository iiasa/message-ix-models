from . import operator

_warned = False


def __getattr__(name):
    global _warned
    if not _warned:
        from warnings import warn

        warn(
            f"Importing from {__name__} is deprecated and will be removed on or after "
            "2024-02-04; use message_ix_models.report.operator instead.",
            DeprecationWarning,
            2,
        )
        _warned = True

    return getattr(operator, name)
