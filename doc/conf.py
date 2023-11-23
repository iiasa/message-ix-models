# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full list see
# the documentation: https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import sphinx.application

# -- Project information ---------------------------------------------------------------

project = "message-ix-models"
copyright = "2020–2023, IIASA Energy, Climate, and Environment (ECE) Program"
author = "IIASA Energy, Climate, and Environment (ECE) Program"


# -- General configuration -------------------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be extensions coming
# with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    # "ixmp.util.sphinx_linkcode_github",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinxcontrib.bibtex",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_template"]

# List of patterns, relative to source directory, that match files and directories to
# ignore when looking for source files. This pattern also affects html_static_path and
# html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

nitpicky = True
nitpick_ignore_regex = {
    # These occur because there is no .. py:module:: directive for the *top-level*
    # module or package in the respective documentation and inventories.
    # TODO Remove once the respective docs are fixed
    ("py:mod", "ixmp"),
    ("py:mod", "message_ix"),
    ("py:mod", "message_data"),
    # iam-units has no Sphinx docs
    ("py:.*", "iam_units.*"),
    # These are a consequence of autosummary-class.rst
    ("py:(obj|meth)", r".*\.Test.*\.test_.*"),
}

rst_prolog = """
.. role:: py(code)
   :language: python

.. |Code| replace:: :class:`~sdmx.model.common.Code`
.. |Platform| replace:: :class:`~ixmp.Platform`
.. |Scenario| replace:: :class:`~message_ix.Scenario`

.. |n| replace:: :math:`n`
.. |y| replace:: :math:`y`
.. |y0| replace:: :math:`y_0`
"""


def setup(app: "sphinx.application.Sphinx") -> None:
    """Copied from pytest's conf.py to enable intersphinx references to these."""
    app.add_crossref_type(
        "fixture",
        "fixture",
        objname="built-in fixture",
        indextemplate="pair: %s; fixture",
    )


# -- Options for HTML output -----------------------------------------------------------

# A list of CSS files.
html_css_files = ["custom.css"]

html_favicon = "_static/favicon.svg"

# The name of an image file (relative to this directory) to place at the top of the
# sidebar.
html_logo = "_static/combined-logo-white.png"

# Add any paths that contain custom static files (such as style sheets) here, relative
# to this directory. They are copied after the builtin static files, so a file named
# "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# The theme to use for HTML and HTML Help pages.  See the documentation for a list of
# builtin themes.
html_theme = "sphinx_rtd_theme"

# -- Options for sphinx.ext.autosummary ------------------------------------------------

autosummary_generate = True

# -- Options for sphinx.ext.extlinks ---------------------------------------------------

extlinks = {
    "issue": ("https://github.com/iiasa/message-ix-models/issue/%s", "GH #%s"),
    "pull": ("https://github.com/iiasa/message-ix-models/pull/%s", "PR #%s"),
}

# -- Options for sphinx.ext.intersphinx ------------------------------------------------


def local_inv(name: str, *parts: str) -> Optional[str]:
    """Construct the path to a local intersphinx inventory."""
    if 0 == len(parts):
        parts = ("doc", "_build", "html")

    from importlib.util import find_spec

    spec = find_spec(name)
    if spec and spec.origin:
        return str(Path(spec.origin).parents[1].joinpath(*parts, "objects.inv"))


# For message-data, see: https://docs.readthedocs.io/en/stable/guides
# /intersphinx.html#intersphinx-with-private-projects
_token = os.environ.get("RTD_TOKEN_MESSAGE_DATA", "")

intersphinx_mapping = {
    "click": ("https://click.palletsprojects.com/en/8.1.x/", None),
    "genno": ("https://genno.readthedocs.io/en/stable", None),
    "ixmp": ("https://docs.messageix.org/projects/ixmp/en/latest/", None),
    "message-ix": ("https://docs.messageix.org/en/latest/", None),
    "m-data": (
        f"https://{_token}:@docs.messageix.org/projects/models-internal/en/latest/",
        # Use a local copy of objects.inv, if the user has one
        (local_inv("message_data"), None),
    ),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable/", None),
    "pint": ("https://pint.readthedocs.io/en/stable/", None),
    "pooch": ("https://www.fatiando.org/pooch/latest/", None),
    "pytest": ("https://docs.pytest.org/en/stable/", None),
    "python": ("https://docs.python.org/3/", None),
    "sdmx": ("https://sdmx1.readthedocs.io/en/stable/", None),
}

# -- Options for sphinx.ext.linkcode / ixmp.util.sphinx_linkcode_github ----------------

linkcode_github_repo_slug = "iiasa/message-ix-models"

# -- Options for sphinx.ext.napoleon ---------------------------------------------------

napoleon_preprocess_types = True
napoleon_type_aliases = {
    "Code": ":class:`~sdmx.model.common.Code`",
    "Path": ":class:`~pathlib.Path`",
    "PathLike": ":class:`os.PathLike`",
}

# -- Options for sphinx.ext.todo -------------------------------------------------------

todo_include_todos = True

# -- Options for sphinxcontrib.bibtex --------------------------------------------------

bibtex_bibfiles = ["main.bib"]
