MESSAGE Ecosystem Documentation
===============================

Dependencies
------------

1. `Sphinx <http://sphinx-doc.org/>`_ v1.1.2 or higher
1. `sphinxcontrib-bibtex <https://sphinxcontrib-bibtex.readthedocs.org/en/latest/>`_

Writing in Restructed Text
--------------------------

There are a number of guides out there, e.g.

- `sphinx <http://www.sphinx-doc.org/en/stable/>`_
- `docutils <http://docutils.sourceforge.net/docs/user/rst/quickref.html>`_

Adding Citations
----------------

The main citation file is `bibs/main.bib`. To learn how to cite those
references, you can read the appropriate `documentation
<http://sphinxcontrib-bibtex.readthedocs.org/en/latest/usage.html>`_.

Adding Packages for Latex
-------------------------

Sometimes building latex requires the addition of `\usepackage{}`
statements. These can be added at the
[preamble line](https://github.com/iiasa/message_doc/blob/master/source/conf.py#L223)
in `source/conf.py`.

Building the Site
-----------------

On *nix, from the command line, run::

    make html

On Windows, from the command line, run::

    ./make.bat html

View the Site Locally
---------------------

On *nix, from the command line, run::

    make serve

On Windows, from the command line, run::

    ./make.bat serve

You can then view the site by pointing your browser at http://localhost:8000/

Automatically Update Website after Changing Files
-------------------------------------------------

If you have `sphinx-autobuild <https://pypi.python.org/pypi/sphinx-autobuild>`_
installed, you make get an auto-updating local website by

    make livehtml

or

    ./make.bat livehtml

Again, point your browser to http://localhost:8000/
