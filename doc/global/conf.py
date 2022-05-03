# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------

project = "message_doc"
copyright = "2016–2020, IIASA Energy, Climate, and Environment (ECE) Program"
author = "IIASA Energy, Climate, and Environment (ECE) Program"

# Set this to the specific version number for a release; otherwise `latest`
version = "2020"
release = "2020"

# -- General configuration ------------------------------------------------

exclude_patterns = [
    "README.rst",
    # Uncomment this line to suppress warnings when these files are excluded.
    # See corresponding comment at the bottom of index.rst.
    "_extra/*.rst",
    # Currently under development
    "glossary.rst",
]

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.todo",
    "sphinxcontrib.bibtex",
]

# Figures, tables and code-blocks are automatically numbered if they have a
# caption.
numfig = True
math_numfig = True
math_eqref_format = "Eq.{number}"

# A string of reStructuredText included at the beginning of every source file
# that is read.
rst_prolog = r"""
.. |MESSAGEix| replace:: MESSAGE\ :emphasis:`ix`

.. role:: strike
.. role:: underline
"""

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "sphinx_rtd_theme"

html_logo = "_static/combined-logo-white.png"

html_static_path = ["_static"]

html_style = "css/custom.css"


# -- Options for LaTeX output ---------------------------------------------

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

# -- Options for sphinx.ext.intersphinx --------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "message_ix": ("https://docs.messageix.org/en/latest/", None),
}

# -- Options for sphinxcontrib.bibtex -----------------------------------------

bibtex_bibfiles = [
    "bibs/main.bib",
    "bibs/messageix-globiom.bib",
]
