Installation
************

.. note:: :mod:`message_ix_models` requires :mod:`message_ix` to run.
   Please ensure your system has :ref:`their required dependencies <message-ix:system-dependencies>` installed.

:mod:`message_ix_models` is structured as a Python package and is published to the PyPI public code repository.
Hence, there are two options for the installation:

From PyPI
---------

This option is only recommended for users who do not wish to make any changes to the source code.

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


Dependencies
------------

See :file:`pyproject.toml`.
The following sets of extra dependencies are available; per the user guide linked above, they can be installed along with the mandatory dependencies by adding (for instance) ``extra_name`` to the package spec :program:`pip install message_data[extra_name]`.

``docs``
   Minimum requirements for building the docs.
``report``
   For running the :doc:`api/report/index` functionality.
``tests``
   Minimal requirements for the test suite.


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
