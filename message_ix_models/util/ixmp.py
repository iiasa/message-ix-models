try:
    # ixmp 3.8.0 and later
    from ixmp.report.util import get_reversed_rename_dims
    from ixmp.util import (
        discard_on_error,
        maybe_check_out,
        maybe_commit,
        parse_url,
        show_versions,
    )
except ImportError:
    # ixmp <= 3.7.0
    from contextlib import nullcontext

    from ixmp.reporting.util import (  # type: ignore [import-not-found,no-redef]
        get_reversed_rename_dims,
    )
    from ixmp.utils import (  # type: ignore [import-not-found,no-redef]
        maybe_check_out,
        maybe_commit,
        parse_url,
        show_versions,
    )

    def discard_on_error(*args):
        return nullcontext()


__all__ = [
    "get_reversed_rename_dims",
    "maybe_check_out",
    "maybe_commit",
    "parse_url",
    "rename_dims",
    "show_versions",
]


def rename_dims() -> dict[str, str]:
    """Access :data:`.ixmp.report.common.RENAME_DIMS`.

    This provides backwards-compatibility with ixmp versions 3.7.0 and earlier. It can
    be removed when message-ix-models no longer supports versions of ixmp older than
    3.8.0.
    """
    try:
        # ixmp 3.8.0 and later
        import ixmp.report.common
    except ImportError:
        # ixmp <= 3.7.0
        import ixmp.reporting.util  # type: ignore [import-not-found]

        return ixmp.reporting.util.RENAME_DIMS
    else:
        return ixmp.report.common.RENAME_DIMS
