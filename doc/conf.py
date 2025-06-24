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
copyright = "2020–%Y, IIASA Energy, Climate, and Environment (ECE) Program"
author = "IIASA Energy, Climate, and Environment (ECE) Program"


# -- General configuration -------------------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be extensions coming
# with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.autosummary",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    # Others
    "genno.compat.sphinx.rewrite_refs",
    # "ixmp.util.sphinx_linkcode_github",  # TODO Address build errors, then enable
    "sphinxcontrib.bibtex",
]

# Figures, tables and code-blocks are automatically numbered if they have a caption
numfig = True
math_numfig = True
math_eqref_format = "Eq.{number}"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_template"]

# List of patterns, relative to source directory, that match files and directories to
# ignore when looking for source files. This pattern also affects html_static_path and
# html_extra_path.
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    # See corresponding comment at the bottom of global/index.rst
    "global/_extra/*.rst",
    # Currently under development
    "global/glossary.rst",
]

nitpicky = True
nitpick_ignore_regex = {
    # Legacy reporting docstrings are not formatted
    ("py:.*", r"boolean \(default|str \(default|None\)|False\)"),
    # iam-units has no Sphinx docs
    ("py:.*", "iam_units.*"),
    # These are a consequence of autosummary-class.rst
    ("py:(obj|meth)", r".*\.Test.*\.test_.*"),
}

# A string of reStructuredText included at the beginning of every source file
rst_prolog = r"""
.. role:: py(code)
   :language: python
.. role:: strike
.. role:: underline

.. |c| replace:: :math:`c`
.. |l| replace:: :math:`l`
.. |n| replace:: :math:`n`
.. |t| replace:: :math:`t`
.. |y| replace:: :math:`y`
.. |y0| replace:: :math:`y_0`

.. |MESSAGEix| replace:: MESSAGE\ :emphasis:`ix`

.. |gh-350| replace::
   This documentation and the related code was migrated from :mod:`message_data` ``dev``
   branch as of commit 8213e6c (2025-05-08) in :pull:`350`.
   It does not reflect further changes made on the :mod:`message_data` ``main`` branch,
   other related branches, or in forks or other repositories.

.. |ssp-scenariomip| replace::
   :ref:`SSP 2023–2025 <ssp-2024>` / :doc:`/project/scenariomip`
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

# -- Options for LaTeX output ----------------------------------------------------------

# LaTeX engine to build the docs
latex_engine = "lualatex"

latex_elements = {
    # Paper size option of the document class.
    "papersize": "a4paper",
    # Additional preamble content.
    "preamble": r"""
    \usepackage{tabularx}
    """,
}

# The name of an image file (relative to this directory) to place at the top of
# the title page.
latex_logo = "_static/logo_blue.png"

# -- Options for genno.compat.sphinx.rewrite_refs --------------------------------------

# When base classes in upstream (genno, ixmp) packages are inherited in message_ix,
# Sphinx will not properly resolve relative references within docstrings of methods of
# the former. Some of these aliases are to allow Sphinx to locate the correct targets.
reference_aliases = {
    # genno
    "Computer": "genno.Computer",
    "KeyLike": ":data:`genno.core.key.KeyLike`",
    r"(genno\.|)Key(?=Seq|[^\w]|$)": "genno.core.key.Key",
    r"(genno\.|)Quantity": "genno.core.attrseries.AttrSeries",
    # ixmp
    "Platform": "ixmp.Platform",
    "TimeSeries": "ixmp.TimeSeries",
    # message_ix
    r"Scenario(?=[^\w]|$)": "message_ix.Scenario",
    "Reporter": "message_ix.report.Reporter",
    "make_df": "message_ix.util.make_df",
    # sdmx
    "Code": "sdmx.model.common.Code",
    #
    # Many projects (including Sphinx itself!) do not have a py:module target in for the
    # top-level module in objects.inv. Resolve these using :doc:`index` or similar for
    # each project.
    "pint$": ":std:doc:`pint <pint:index>`",
    "plotnine$": ":class:`plotnine.ggplot`",
}

# -- Options for sphinx.ext.autosummary ------------------------------------------------

autosummary_generate = True

# -- Options for sphinx.ext.extlinks ---------------------------------------------------

extlinks = {
    "issue": ("https://github.com/iiasa/message-ix-models/issue/%s", "GH #%s"),
    "pull": ("https://github.com/iiasa/message-ix-models/pull/%s", "PR #%s"),
    "gh-user": ("https://github.com/%s", "@%s"),
    "source": ("https://github.com/iiasa/message-ix-models/blob/main/%s", "%s"),
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
    else:
        return None


# For message-data, see: https://docs.readthedocs.io/en/stable/guides/intersphinx.html#intersphinx-with-private-projects
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
    "platformdirs": ("https://platformdirs.readthedocs.io/en/latest", None),
    "pooch": ("https://www.fatiando.org/pooch/latest/", None),
    "pyam": ("https://pyam-iamc.readthedocs.io/en/stable", None),
    "pytest": ("https://docs.pytest.org/en/stable/", None),
    "python": ("https://docs.python.org/3/", None),
    "sdmx": ("https://sdmx1.readthedocs.io/en/stable/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master", None),
}

# -- Options for sphinx.ext.linkcode / ixmp.util.sphinx_linkcode_github ----------------

linkcode_github_repo_slug = "iiasa/message-ix-models"

# -- Options for sphinx.ext.napoleon ---------------------------------------------------

napoleon_preprocess_types = True
napoleon_type_aliases = {
    # Python standard library
    "iterable": ":class:`~collections.abc.Iterable`",
    "sequence": ":class:`~collections.abc.Sequence`",
    "PathLike": ":class:`os.PathLike`",
    # genno
    "AnyQuantity": ":data:`~genno.core.quantity.AnyQuantity`",
}

# -- Options for sphinx.ext.todo -------------------------------------------------------

# If true, `todo` and `todoList` produce output, else they produce nothing
todo_include_todos = True

# -- Options for sphinxcontrib.bibtex --------------------------------------------------

bibtex_bibfiles = ["main.bib", "messageix-globiom.bib"]
