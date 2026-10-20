import sys
from os import environ
from pathlib import Path

from packaging.version import InvalidVersion
from packaging.version import Version as V

upstream_version, postgresql_uri = sys.argv[1:]

v_main = V("999")  # Corresponds to upstream 'main'
try:
    version = V(upstream_version)
except InvalidVersion:
    version = v_main

result = "dependencies="
for condition, dependency in (
    # Upstream versions < 3.9.0 use hook arguments
    # removed in pytest 8.1.0 (#155) and 9.0.0
    (version < V("3.9"), "pytest < 8.1"),
    # https://github.com/iiasa/message-ix-models/issues/366
    (V("3.8") < version < V("3.11"), "ixmp4 < 0.11"),
    # ixmp.util.ixmp4 contains compatibility code that only
    # works with pandas < 3.
    (version == V("3.11"), "pandas < 3"),
    # Work around https://github.com/iiasa/ixmp4/issues/272
    (version == v_main, "sqlalchemy < 2.1"),
):
    result += f' "{dependency}"' if condition else ""

result += "\npytest-args="
if version == v_main:
    result += f'--ixmp-postgres="{postgresql_uri}"'

Path(environ["GITHUB_OUTPUT"]).write_text(result)
