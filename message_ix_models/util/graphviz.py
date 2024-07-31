from subprocess import DEVNULL, check_call

try:
    from graphviz import DOT_BINARY
except ImportError:
    DOT_BINARY = "dot"

try:
    check_call([DOT_BINARY, "-V"], stdout=DEVNULL, stderr=DEVNULL)
except FileNotFoundError:
    #: :any:`.True` if the :program:`graphviz` programs are installed, as required by
    #: :mod:`.graphviz` and :meth:`genno.Computer.visualize`.
    HAS_GRAPHVIZ = False
else:
    HAS_GRAPHVIZ = True
