# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------

project = 'MESSAGE-GLOBIOM'
copyright = '2016–2020, IIASA Energy Program'
author = 'IIASA Energy Program'
# The major project version, used as the replacement for |version|.
version = '2020-03-05'
# The full project version, used as the replacement for |release|.
release = '2020-03-05'

# -- General configuration ------------------------------------------------

exclude_patterns = ["README.rst"]

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.coverage',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.mathjax',
    'sphinx.ext.todo',
    'sphinxcontrib.bibtex',
]

# Figures, tables and code-blocks are automatically numbered if they have a
# caption.
numfig = True

# A string of reStructuredText included at the beginning of every source file
# that is read.
rst_prolog = """
.. role:: strike
.. role:: underline
"""

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_rtd_theme'

html_logo = '_static/logo_white.png'

html_static_path = ['_static']

html_style = 'css/custom.css'


# -- Options for HTML help output -----------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = 'messagedoc'


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # Paper size option of the document class.
    'papersize': 'a4paper',

    # Additional preamble content.
    'preamble': r"""
    \usepackage{tabularx}
    """,
}

# Group the document tree into LaTeX source files.
# latex_documents = [
#     ('index', 'message.tex', 'MESSAGE-GLOBIOM Documentation',
#      'IIASA Energy Program', 'manual', False),
# ]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
latex_logo = '_static/logo_blue.png'

# -- Options for sphinx.ext.intersphinx --------------------------------------

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'message_ix': ('https://docs.messageix.org/en/latest/', None),
}
