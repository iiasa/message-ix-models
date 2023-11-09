Command-line interface
**********************

This page describes how to use the :program:`mix-models` command-line interface (CLI) to perform common tasks.
:program:`mix-models` is organized into **commands** and **subcommands**, sometimes in multiple levels.
Our goal is that the *semantics* of all commands are similar, so that interacting with each command feels similar.

.. contents::
   :local:


Controlling CLI behaviour
=========================

To support a variety of complex use-cases, the MESSAGEix stack takes configuration and inputs from several places:

:mod:`ixmp` configuration file: :file:`config.json`
---------------------------------------------------
:mod:`ixmp` keeps track of named Platforms and their associated databases, and stores information in its :file:`config.json` file.
See :mod:`ixmp.config`.
List existing platforms::

    $ ixmp platform list

To add a specific database, you can use the ixmp CLI [1]_::

    $ ixmp platform add [PLATFORMNAME] jdbc oracle [COMPUTER]:[PORT]/[SERVICENAME] [USERNAME] [PASSWORD]

You may also want to make this the *default* platform.
Unless told otherwise, :mod:`message_ix_models` creates :class:`~ixmp.Platform` objects without any arguments (``mp = ixmp.Platform()``); this loads the default platform.
Set the default::

    $ ixmp platform add default [PLATFORMNAME]

:mod:`message_ix` stores only one configuration value in :file:`config.json`: ``'message model dir'``, the path to the GAMS model files.
MESSAGEix-GLOBIOM uses the GAMS model files from the current :mod:`message_ix` ``master`` branch, so you should not set this, or unset it when using :mod:`message_ix_models`.

:mod:`message_ix_models` will use the :file:`config.json` value ``"message_local_data"`` for local data, if it is set and not overridden.

.. [1] ``[COMPUTER]`` is in this case either the hostname or the IP address.

Environment variables
---------------------
Some code responds to environment variables.
For example, ixmp responds to ``IXMP_DATA``, which tells it where to find the file :file:`config.json`.

:mod:`message_ix_models` responds to ``MESSAGE_LOCAL_DATA``; see :ref:`the discussion of local data <local-data>`.


CLI parameters (arguments and options)
--------------------------------------

Each command has zero or more arguments and options.
**Arguments** are mandatory and follow the command name in a certain order.
**Options**, as the name implies, are not required.
If an option is omitted, a default value is used; the code and ``--help`` text make clear what the default behaviour is.

Arguments and options are **hierarchical**.
Consider the following examples::

    $ mix-data --opt0=foo cmd1 --opt1=bar arg1 cmd2 --opt2=baz arg2
    $ mix-data --opt0=foo cmd1            arg1 cmd3 --opt3=baz arg3a arg3b

In these examples:

- ``--opt0`` is an option that (potentially) affects **any** command, including the subcommands ``cmd2`` or ``cmd3``.
- ``--opt1`` and ``arg1`` are an option and mandatory argument to the command ``cmd1``.
  They might not have any relevance to other ``mix-data`` commands.
- ``cmd2`` and ``cmd3`` are distinct subcommands of ``cmd1``.

  - They *may* respond to ``--opt1`` and ``arg1``, and to ``--opt0``; at least, they *must* not contradict them.
  - They each may have their own options and arguments, which can be distinct.

.. tip:: Use ``--help`` for any (sub)command to read about its behaviour.
   If the help text does not make the behaviour clear, `file an issue <https://github.com/iiasa/message-ix-models/issues/new>`_.

Configuration files and metadata
--------------------------------
For some features of the code, the default behaviour is very elaborate and serves for most uses; but we also provide the option to override it.
This default behaviour or optional behaviour is defined by reading an input file.
These are stored in the :ref:`package data <package-data>` directory.

For example, ``mix-models report`` loads reporting configuration from :file:`message_ix_models/data/report/global.yaml`, a YAML file with hundreds of lines.
Optionally, a different file can be used::

    $ mix-models report --config other

…looks for a file :file:`other.yaml` in the :ref:`local data <local-data>` directory or current working directory. Or::

    $ mix-models report --config /path/to/another/file.yaml

…can be used to point to a file in a different directory.


Important CLI options and commands
==================================

.. _cli-help:

Top-level options and commands
------------------------------
``mix-models --help`` describes these::

    $ mix-models --help
    Usage: mix-models [OPTIONS] COMMAND [ARGS]...

      Command-line interface for MESSAGEix-GLOBIOM model tools.

      Every tool and script in this repository is accessible through this CLI.
      Scripts are grouped into commands and sub-commands. For help on specific
      (sub)commands, use --help, e.g.:

              mix-models cd-links --help
              mix-models cd-links run --help

      The top-level options --platform, --model, and --scenario are used by
      commands that access specific message_ix scenarios; these can also be
      specified with --url.

      For more information, see
      https://docs.messageix.org/projects/models2/en/latest/cli.html

    Options:
      --url ixmp://PLATFORM/MODEL/SCENARIO[#VERSION]
                                      Scenario URL.
      --platform PLATFORM             Configured platform name.
      --model MODEL                   Model name for some commands.
      --scenario SCENARIO             Scenario name for some commands.
      --version INTEGER               Scenario version.
      --local-data PATH               Base path for local data.
      -v, --verbose                   Print DEBUG-level log messages.
      --help                          Show this message and exit.

    Commands:
      cd-links         CD-LINKS project.
      dl               Retrieve data from primary sources.
      engage           ENGAGE project.
      iiasapp          Import power plant capacity.
      material         Model with materials accounting.
      prep-submission  Prepare scenarios for submission to database.
      report           Postprocess results.
      res              MESSAGE-GLOBIOM reference energy system (RES).
      techs            Export data from data/technology.yaml to CSV.
      transport        MESSAGEix-Transport variant.

To explain further:

``--platform PLATFORM`` or ``--url``
   By default, message_data connects to the default ixmp Platform.
   These options direct it to work with a different Platform.

``--model MODEL --scenario SCENARIO`` or ``--url``
    Many commands use an *existing* |Scenario| as a starting point, and begin by cloning that Scenario to a new (model name, scenario name).
    For any such command, these top-level options define the starting point/initial Scenario to clone/‘baseline’.

    In contrast, see ``--output-model``, below.


Common options
--------------
Since :mod:`message_ix_models.model` and :mod:`message_ix_models.project` codes often perform similar tasks, their CLI options and arguments are provided in :mod:`.util.click` for easy re-use.
These include:

``ssp`` argument
   This takes one of the values 'SSP1', 'SSP2', or 'SSP3'.

   Commands that will not work for one or more of the SSPs should check the argument value given by the user and raise :class:`NotImplementedError`.

``--output-model NAME`` option
   This option is a counterpart to the top-level ``--url/--model/--scenario`` options.
   A command that starts from one Scenario, and builds one or more Scenarios from it will clone *to* a new (model name, scenario name);
   ``--output-model`` gives the model name.

   Current code generates a variety of fixed (non-configurable) scenario names; use ``--help`` for each command to see which.


To employ these in new code, refer to the example of existing code.
