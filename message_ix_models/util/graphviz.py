"""Utilities for working with :program:`graphviz` and :mod:`graphviz`."""

from subprocess import DEVNULL, CalledProcessError, check_call

#: :any:`.True` if both :mod:`graphviz` and the :program:`graphviz` programs are
#: installed, as required by :meth:`genno.Computer.visualize` and others.
HAS_GRAPHVIZ = True

try:
    from graphviz import DOT_BINARY
except ImportError:
    DOT_BINARY = "dot"
    HAS_GRAPHVIZ = False

try:
    check_call([DOT_BINARY, "-V"], stdout=DEVNULL, stderr=DEVNULL)
except (CalledProcessError, FileNotFoundError):
    HAS_GRAPHVIZ = False
