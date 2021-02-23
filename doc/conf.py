# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full list see
# the documentation: https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information ---------------------------------------------------------------

project = '"MESSAGEix models"'
copyright = "2021, IIASA Energy, Climate, and Environment (ECE) Program"
author = "IIASA Energy, Climate, and Environment (ECE) Program"


# -- General configuration -------------------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be extensions coming
# with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and directories to
# ignore when looking for source files. This pattern also affects html_static_path and
# html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -----------------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for a list of
# builtin themes.
html_theme = "sphinx_rtd_theme"

# A list of CSS files.
html_css_files = ["custom.css"]

# The name of an image file (relative to this directory) to place at the top of the
# sidebar.
html_logo = "_static/logo-white.png"

# Add any paths that contain custom static files (such as style sheets) here, relative
# to this directory. They are copied after the builtin static files, so a file named
# "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]


# -- Options for sphinx.ext.extlinks ---------------------------------------------------

extlinks = {
    "issue": ("https://github.com/iiasa/message-ix-models/issue/%s", "GH #"),
    "pull": ("https://github.com/iiasa/message-ix-models/pull/%s", "PR #"),
}


# -- Options for sphinx.ext.intersphinx ------------------------------------------------

intersphinx_mapping = {
    "ixmp": ("https://docs.messageix.org/projects/ixmp/en/latest/", None),
    "message-ix": ("https://docs.messageix.org/projects/ixmp/en/latest/", None),
    "m-data": ("https://docs.messageix.org/projects/global/en/latest/", None),
    "python": ("https://docs.python.org/3/", None),
    "sdmx": ("https://sdmx1.readthedocs.io/en/stable/", None),
}
