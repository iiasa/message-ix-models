MESSAGEix-GLOBIOM
=================

.. image:: https://readthedocs.com/projects/iiasa-energy-program-message-doc/badge/?version=2019-update
   :target: https://message.iiasa.ac.at/projects/global/en/2019-update/?badge=2019-update
   :alt: Documentation Status

Documentation for the MESSAGEix-GLOBIOM global integrated assessment model.

Dependencies
------------

1. `Sphinx <http://sphinx-doc.org/>`_.
2. `sphinx_rtd_theme <https://sphinx-rtd-theme.readthedocs.io/>`_.
3. `sphinxcontrib-bibtex <https://sphinxcontrib-bibtex.readthedocs.io/>`_.

Install using ``pip install -r requirements.txt``.


Writing
-------

- `Quick reStructuredText <http://docutils.sourceforge.net/docs/user/rst/quickref.html>`_ reference from docutils.
- `ReST cheat sheet <https://thomas-cokelaer.info/tutorials/sphinx/rest_syntax.html>`_ by Thomas Cokelaer.
- `Sphinx <http://www.sphinx-doc.org/>`_ documentation, including reference on Sphinx-specific ReST syntax.
- `Usage <https://sphinxcontrib-bibtex.readthedocs.io/en/latest/usage.html>`_ for ``sphinxcontrib.bibtex`` (citations and bibliography).
  - Add references to ``source/bibs/main.bib``. Format entries:
    - Field lines like ``[TAB]year = {2010},``: tab indent, spaces around ``=``, value inside ``{}``, trailing comma.
    - ``keywords`` separated by semicolons (';').
    - Do not include ``localfile`` or ``file``  fields.


Viewing the docs
----------------

On Read The Docs
~~~~~~~~~~~~~~~~

- Branches other than ``master`` in the main repository (``iiasa/message_doc``) can be built under the IIASA ENE ReadTheDocs account, and will appear at ``https://message.iiasa.ac.at/en/[BRANCH]``.
  Authorized users can configure these at https://readthedocs.com/projects/iiasa-energy-program-message-doc/versions/
- For a personal fork, e.g. ``[USER]/message_doc``, visit https://readthedocs.io [1] and configure builds for your fork, using a project name like ``message-doc-[USER]``.
  These will appear at ``https://message-doc-[USER].readthedocs.io/en/[BRANCH]``.


.. [1] note that this is the free/open-source version of RTD, rather than the commercial product used by ENE for the official documentation of releases.

Locally
~~~~~~~

Build from the command line on Linux or macOS::

    make html

or, on Windows::

    .\make html


View the built documentation on Linux or macOS::

    make serve

or, on Windows::

    .\make serve

You can then view the site by pointing your browser at http://localhost:8000/

Optionally, install `sphinx-autobuild <https://pypi.org/[project]/sphinx-autobuild>`_, which will rebuild the documentation any time a file is modified.
Then, on Linux or mac OS::

    make livehtml

or, on Windows::

    .\make livehtml

Again, point your browser to http://localhost:8000/
