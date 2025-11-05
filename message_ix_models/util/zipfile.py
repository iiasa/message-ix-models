import logging
from pathlib import Path
from tarfile import TarFile
from zipfile import ZipFile

log = logging.getLogger(__name__)


class Archive:
    """Unified interface to :class:`tarfile.TarFile` and :class:`zipfile.ZipFile`."""

    path: Path
    _cls: type[TarFile | ZipFile]
    _file: TarFile | ZipFile

    def __init__(self, path: Path) -> None:
        self.path = path
        if path.suffix == ".zip":
            self._cls = ZipFile
        elif path.suffix in (".gz", ".xz"):
            self._cls = TarFile
        else:
            raise ValueError(f"unsupported suffix {path.suffix!r}")

    def __enter__(self) -> "Archive":
        self._file = self._cls(self.path)
        return self

    def __exit__(self, exc_type, exc_val: BaseException | None, exc_tb) -> None:
        try:
            self._file.close()
        finally:
            if exc_val:
                raise exc_val

    def extract(self, *args, **kwargs) -> Path:
        result = self._file.extract(*args, **kwargs)
        assert result is not None
        return Path(result)

    def getinfo(self, name: str) -> tuple[str, int]:
        if isinstance(self._file, TarFile):
            ti = self._file.gettarinfo(name)
            return ti.name, ti.size
        elif isinstance(self._file, ZipFile):
            zi = self._file.getinfo(name)
            return zi.filename, zi.file_size

    def getnames(self) -> list[str]:
        if isinstance(self._file, TarFile):
            return self._file.getnames()
        elif isinstance(self._file, ZipFile):
            return self._file.namelist()


def extract_if_newer(
    path: Path,
    target_dir: Path = Path("."),
    members: list[str] | None = None,
) -> list[Path]:
    """Extract all members from an archive at `path` to `target_dir`.

    For each member, if the target path exists and is the same size as expected,
    extraction is skipped.

    .. todo:: Extend to use a configurable set of attributes (including 0 or more of
       size, mtime, etc.) to determine whether to extract.
    """

    # Identify the directory for extracted files
    if not target_dir.is_absolute():
        # A relative path, possibly the default
        target_dir = path.parent.joinpath(target_dir)

    # Ensure the directory exists
    target_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Decompress {path} to {target_dir}")
    result, skip = [], []
    with Archive(path) as archive:
        # Identify members to be unpacked
        if not members:
            members = archive.getnames()
            log.info(f"Extract all {len(members)} members of {path}")

        for member in members:
            filename, size = archive.getinfo(member)

            # Candidate path for the extracted file
            target = target_dir.joinpath(filename)
            if target.exists() and target.stat().st_size >= size:
                result.append(target)
                skip.append(target)
            else:
                result.append(archive.extract(member, path=target_dir))

    if skip:
        log.info(f"Skipped {len(skip)} members")

    return result
