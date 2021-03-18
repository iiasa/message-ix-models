MESSAGEix-GLOBIOM documentation
*******************************

.. image:: https://readthedocs.com/projects/iiasa-energy-program-message-doc/badge/?version=master
   :target: https://docs.messageix.org/projects/global/?badge=master
   :alt: Documentation Status

This is a `Sphinx <http://sphinx-doc.org/>`__ project for documentation of the MESSAGEix-GLOBIOM global integrated assessment model (IAM).
The built documentation is at:

- https://docs.messageix.org/projects/global/ or
- https://docs.messageix.org/global/

Build
=====

on ReadTheDocs
--------------

- Branches other than ``master`` in the main repository (``iiasa/message_doc``) can be built under the IIASA ECE ReadTheDocs account, and will appear at ``https://docs.messageix.org/projects/global/en/[BRANCH]``.
  Authorized users can configure these at https://readthedocs.com/projects/iiasa-energy-program-message-doc/versions/
- For a personal fork, e.g. ``[USER]/message_doc``, visit https://readthedocs.io [1]_ and configure builds for your fork, using a project name like ``message-doc-[USER]``.
  These will appear at ``https://message-doc-[USER].readthedocs.io/en/[BRANCH]``.

.. [1] note that this is the free/open-source version of RTD, rather than the commercial product used by ECE for the official documentation of releases.


Locally
-------

1. Install Sphinx and other requirements (
     `sphinx_rtd_theme <https://sphinx-rtd-theme.readthedocs.io/>`_ and
     `sphinxcontrib-bibtex <https://sphinxcontrib-bibtex.readthedocs.io/>`_
   )::

      pip install -r requirements.txt

2. Build from the command line. On Linux or macOS::

    make html

   On Windows::

    .\make html

3. Open ``_build/index.html``.


Write
=====

Use the following references:

- `Quick reStructuredText <http://docutils.sourceforge.net/docs/user/rst/quickref.html>`_ reference from docutils.
- `ReST cheat sheet <https://thomas-cokelaer.info/tutorials/sphinx/rest_syntax.html>`_ by Thomas Cokelaer.
- `Sphinx <http://www.sphinx-doc.org/>`__ documentation, including reference on Sphinx-specific ReST syntax.
- `Usage <https://sphinxcontrib-bibtex.readthedocs.io/en/latest/usage.html>`_ for ``sphinxcontrib.bibtex`` (citations and bibliography).

Add citations to ``bibs/main.bib``. Format entries:

- Field lines like ``[TAB]year = {2010},``: tab indent, spaces around ``=``, value inside ``{}``, trailing comma.
- Use either ``doi`` (preferred) or ``url``, not both.
- ``keywords`` separated by semicolons (';').
- Do not include ``abstract``, ``localfile``, or ``file``  fields.
