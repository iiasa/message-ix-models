Installation
************

:mod:`message_ix_models` is structured as a Python package,
is published to the Python Package Index (PyPI) public repository,
and is Free Software.
Hence, there are two options for the installation:

From the Python Package Index (PyPI)
------------------------------------

This option is only recommended for users who do not wish to make any changes to the source code.
This page uses `pip <https://pip.pypa.io>`_;
another alternative is `uv pip <https://docs.astral.sh/uv/reference/cli/#uv-pip>`_.

1. Run::

    $ pip install message-ix-models[<extra_dependencies>]

From source
-----------

Use this option if you intend to make changes to the source code.
We value your contributions via pull requests to `the main repository <https://github.com/iiasa/message-ix-models>`_.
Please consider :doc:`contributing <message-ix:contributing>` your changes.

1. Fork the `the main repository <https://github.com/iiasa/message-ix-models>`_.
   This will create a new repository ``<user>/message-ix-models``.

2. Clone your fork; using the `Github Desktop <https://desktop.github.com>`_ client, or the command line::

    $ git clone git@github.com:USER/message-ix-models.git

3. Add the main repository as a remote git repository.
   This will allow keeping up to date with changes there and importing tags, which also needs to be done for the install tests to succeed::

    $ git remote add upstream git@github.com:iiasa/message-ix-models.git
    $ git fetch upstream --tags

4. Inside the :file:`message-ix-models` directory, run::

    $ pip install --editable .[<extra_dependencies>]

.. _install-dependencies:

Dependencies
------------

The mandatory dependencies of :mod:`message_ix_models` include:

- :mod:`message_ix` and thus :ref:`its upstream dependencies <message-ix:system-dependencies>`.

The following ‘extras’, or sets of optional dependencies, are available.
Without the associated dependencies,
the respective modules or features will not work:
for example, it is optional to use :doc:`material/index`,
but in order to use it the ``material`` extra dependencies **must** be installed.

Per the :program:`pip` documentation (linked above),
they can be installed along with the mandatory dependencies by adding to the package spec.
For example, :program:`pip install message_data[material,transport]`
will install the dependencies in those two sets.

``bilateralize``
   For :mod:`.tools.bilateralize`.
``buildings``
   For :doc:`buildings/index`.
``docs``
   Build these documentation pages.
``iea-web``
   For :mod:`.tools.iea.web`.
``material``
   For :doc:`material/index`.
``migrate``
   For :doc:`howto/migrate`.
``report``
   For :doc:`api/report/index`;
   includes :mod:`plotnine` and :mod:`xlsxwriter`.
``tests``
   Run the test suite.
``transport``
   For :doc:`transport/index`.

See :file:`pyproject.toml` in the source for the exact contents of each set.

Check that installation was successful
--------------------------------------

Verify that the version installed corresponds to the `latest release <https://github.com/iiasa/message-ix-models/releases>`_ by running the following commands on the command line::

    # Show versions of message_ix, message-ix-models, and key dependencies
    $ message-ix show-versions

    # Show the list of modelling platforms that have been installed and
    # the path to the database config file
    # By default, just the local database should appear in the list
    $ message-ix platform list
    $ mix-models config show

The above commands will work as of :mod:`message_ix` version 3.0 and in subsequent versions.
Please read through the output of the :ref:`mix-models command <cli-help>` to understand the different CLI options and what you can do with them.
