Configuration and (meta)data
****************************

Many, varied kinds of data are used to prepare and modify MESSAGEix-GLOBIOM scenarios.
Other data are produced by code as incidental or final output.
These can be categorized in several ways.
One is by the purpose they serve:

- **configuration**: settings that affect how code works,
  *where* (meta)data are located,
  *how* they should be processed, etc.
- **data**: actual numerical values used or produced by code,
- **metadata**: information describing how data is structured, separate from the data itself.

Another is by whether the (meta)data are **input**, **output**, or both.

This page describes how configuration and data are handled in :mod:`message_ix_models` and :mod:`message_data`. [1]_
In many cases it also specifies what to do for new additions to the code,
using :ref:`RFC 2119 keywords <message-ix:prereq-rfc2119>` like **must** and **should**.
The :doc:`HOWTO Work with paths to files and data </howto/path>` contains some suggested ways to handle particular situations.

.. [1] Unless specifically distinguished in the text,
   all of the following applies to *both* :mod:`message_ix_models` and :mod:`message_data`.

.. contents::
   :local:
   :depth: 2

Configuration
=============

.. _context:

Context objects
---------------

:class:`.Context` objects are used to carry configuration, environment information, and other data between parts of the code.
Scripts and user code can also store values in a Context object.

There is always at least 1 Context instance available;
if necessary, additional instances can be created to be used for only part of a program.

.. code-block:: python

    # Get an existing instance of Context
    c = Context.get_instance()

    # Store a value using attribute syntax
    c.foo = 42

    # Store a value with spaces in the name using item syntax
    c["PROJECT data source"] = "Source A"

    # my_function() responds to 'foo' or 'PROJECT data source'
    my_function(c)

    # Store a sub-dictionary of values
    c["PROJECT2"] = {"setting A": 123, "setting B": 456}

    # Create a subcontext with all the settings of `c`
    c2 = deepcopy(c)

    # Modify one setting
    c2.foo = 43

    # Run code with this alternate setting
    my_function(c2)

For the :doc:`cli`, every command decorated with :py:`@click.pass_obj` gets a first positional argument :py:`context`,
which is an instance of this class.
The settings are populated based on the command-line parameters given to :program:`mix-models` or its (sub)commands.

.. _core-config:

Core configuration
------------------

The :class:`~message_ix_models.util.config.Config` class (always stored at :py:`context.core`) defines configuration settings used across :mod:`message_ix_models`.
See its documentation for details.
In particular, the settings :attr:`.Config.cache_path` and :attr:`.Config.local_data` are relevant to this page.

Specific modules for model variants, projects, tools, etc. **should…**

- **Define** a :mod:`dataclass <dataclasses>` named Config to express the configuration options they understand.
  See for example:

  - :class:`.model.Config` for describing existing models or constructing new models,
  - :class:`.report.Config` for reporting,
  - :class:`.tools.costs.Config` for a general-purpose tool in a complex module, and
  - :class:`.model.transport.Config` for a particular model variant, here MESSAGEix-Transport.

- **Store** this on the :class:`.Context` at a documented key.
  For example :class:`.model.Config` is stored at :py:`context.model` or :py:`context["model"]`.
  Usually this key **should** match part or all of the module name.
- **Retrieve** and respect configuration from existing objects.

  For example, module-specific code
  that needs to understand which :doc:`node code list <pkg-data/node>` is used by the scenario on which it operates
  **should** retrieve this from :attr:`.model.Config.regions`
  and **should not** create another key/setting to store the same information.

  Config settings with duplicate names **should** only be created and used when they have a different purpose or meaning than existing settings.
- **Control** the behaviour of other modules by setting the appropriate configuration values.

Data and metadata
=================

Locations
---------

:mod:`message_ix_models` contains code and tools for handling the following data locations.
This section gives a brief description of these locations,
using short labels (like “package data”) that also appear elsewhere in this documentation.
The following sections describe how they are and should be used.

.. _user-cache:

User cache
   Typically this is in the user's home directory at a path like :file:`$HOME/.cache/message-ix-models/`.

   :attr:`.Config.cache_path` (equivalently :py:`Context.core.cache_path`) identifies this directory.
   :attr:`.Config.get_cache_path` constructs sub-paths.

.. _package-data:
.. _test-data:

Package data
   These are stored in the :file:`message_ix_models/data/` subdirectory of the `iiasa/message-ix-models <https://github.com/iiasa/message-ix-models>`_ git repository.

   Some of these data are included in the packaged distributions of :mod:`message_ix_models`
   `available on PyPI <https://pypi.org/project/message-ix-models>`_.
   Other files are omitted to keep the size of these distributions small.

   :func:`.package_data_path`, :func:`.load_package_data`, and other more specialized code access this directory and subdirectories.

   Test data
      The directory :file:`message_ix_models/data/test/` contains data that is (only) used for testing.
      Some of the files in this directory mirror the name and structure of data files stored elsewhere, but contain reduced and/or randomized/fuzzed data.

.. _private-data:
.. _static-data:

Private data
   These are stored in two non-public Git repositories.

   :mod:`message_data` repository
      These are stored in the top-level :file:`data/` directory of `iiasa/message_data <https://github.com/iiasa/message_data>`_.
      This repository also contains the :py:`message_data` Python package.
      This repository is not public; and the Python package is not published on or installable from PyPI.
      Users with access to the repository can read more in its :doc:`its documentation <m-data:index>`.

      :func:`.private_data_path`, :func:`.load_private_data`, and other more specialized code access this directory and subdirectories.

   Static private data
      These are stored in `iiasa/message-static-data <https://github.com/iiasa/message-static-data>`_.
      This repo contains specific data files that cannot (currently, or ever) be made public,
      for instance because of restrictive license conditions.
      It contains no code.

      Files are collected in this repository for convenience;
      users who have valid licenses to the data are granted access to the repository.
      In most cases, these data can also be obtained from the original source(s) with an appropriate license, registration, payment, or other conditions.

      See :ref:`HOWTO Connect static data to local data <howto-static-to-local>`.

.. _local-data:

(User-)Local data
   This is any arbitrary path on a user's system.

   :attr:`.Config.local_data` (equivalently :py:`context.core.local_data`) point to this directory.
   :meth:`.Context.get_local_path` and :func:`.local_data_path` construct paths under this directory.

   The path can be set in multiple ways.
   From lowest to highest precedence:

   1. The default location is the *current working directory*:
      the directory in which the :program:`mix-models` :doc:`command-line interface <cli>` is invoked,
      or in which Python code is run that imports and uses :mod:`message_ix_models`.
   2. The :mod:`ixmp` configuration file setting ``message local data``.
      See :ref:`ixmp:configuration` in the ixmp documentation.
   3. The environment variable ``MESSAGE_LOCAL_DATA``.
   4. The :program:`mix-models --local-data=…` CLI option and related options for subcommands,
      for instance :program:`mix-models report --output=…`.
   5. Code that directly modifies the :attr:`.local_data` setting.

.. _data-goes-where:

Choose where to store (meta)data
--------------------------------

Developers of :mod:`message_ix_models` code **must** follow this order of priority in choosing where to store input and output (meta)data.

.. contents::
   :local:

(1) *Not* in :mod:`message_ix_models`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Data that are available from public, stable sources **should not** be added to the :mod:`message_ix_models` repository.
Instead:

1. **Fetch** the code from their original location.
   This **should** be done by extending or using :mod:`message_ix_models.util.pooch`,
   which stores the retrieved files in the :ref:`user cache <user-cache>`.
2. If :mod:`message_ix_models` relies on certain adjustments to the data,
   **do not** commit the adjusted data.
   Instead:

   a. **Commit code** that performs the adjustments.
      This makes methods for data transformation (and any assumptions involved) transparent.
   b. If necessary, store the result in the user cache.

(2) Local data or user cache
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See :ref:`local data <local-data>` and :ref:`user cache <user-cache>` above.
These locations are **recommended** for:

- Outputs, such as data or plot files generated by reporting.
- Caches: temporary data files used to speed up other code by avoiding repeat of slow operations.

These kinds of data **must not** be committed as :mod:`message_ix_models` package data.
Caches and output **should not** be committed as :mod:`message_data` private data.

Thus each user **should** configure a local data path appropriate to their system,
using either the :mod:`ixmp` configuration file or environment variable as described above.
For example:

.. code-block:: shell

   mix-models config set message_local_data /path/to/a/local-data/dir

It is **recommended** to use a directory *outside* any other Git-controlled directories,
for instance clones of :mod:`message_ix_models` or :mod:`message_data`.
(If not, users **should** use :file:`.gitignore` files to hide the local data directory from Git.)

(3) Package data
~~~~~~~~~~~~~~~~

:ref:`See above <package-data>`.
This location is **recommended** for:

- Configuration files used to populate Config classes for specific modules.
- General-purpose metadata for the MESSAGEix-GLOBIOM base global model or variants.
- Data for publicized model variants and completed/published projects.

These files **may** be packaged and published so that they are installable from PyPI with :mod:`message_ix_models`;
configuration and metadata generally **should** be packaged.
Data, if they are large, **may** also be excluded via :file:`MANIFEST.in`.
See :ref:`large-input-data`, below.

**Document** these data in files like :file:`doc/pkg-data/*.rst` that are included in the present documentation,
for example :doc:`pkg-data/node`.

(4) Static data
~~~~~~~~~~~~~~~

This location is **recommended** for data that is subject to license
or other conditions that prohibit their being made public,
especially data provided by other people and organizations.
(If this is *not* the case, store these as package data or fetch them.)

(Sub)directories in ``message-static-data``, if they match directories under :file:`message_ix_models/data/`, **must** have a matching structure.

**Document** these data on the page :doc:`data-sources`
or together with other code modules that handle them.
The documentation **must** indicate the original source and process to obtain the data files.

(5) Private data
~~~~~~~~~~~~~~~~

:ref:`See above <private-data>`.
This location is **recommended** for:

- Data for model variants and projects under current development.
- Specific data files that cannot (currently, or ever) be made public,
  for instance because of restrictive licenses,
  especially in cases where there is no public documentation
  or information about how users could obtain the data.

General recommendations
-----------------------

Always consider: “Will this code work on another researcher's computer?”

Prefer text formats
   …such as CSV, over binary formats like Excel.
   CSV files are compressed by Git automatically,
   and Git can handle diffs to these files easily.
   Code that reads/writes these files is *much* faster,
   especially for files with thousands or more data points.

*Do not* hard-code paths
   :mod:`message_ix_models` utility functions and Config settings allow to access all the (meta)data locations described above.
   It **should not** ever be necessary to use a hard-coded path;
   this is a clue that data are not in a proper location.

   For system-specific paths
   (:ref:`local data <local-data>` and :ref:`user cache <user-cache>`),
   get a :obj:`.Context` object
   and use it to get an appropriate :class:`~pathlib.Path` pointing to a file:

   .. code-block:: python

       # Store a base path
       project_path = context.get_local_path("myproject", "output")

       # Use the Path object to generate a subpath
       run_id = "foo"
       output_file = project_path.joinpath("reporting", run_id, "all.xlsx")

Keep input and output data separate
   Any directory **should** contain either input *or* output data—never both.
   Output data **should not** be stored in :ref:`package data <package-data>`, :ref:`private data <private-data>`, or :ref:`static data <static-data>` paths;
   it **must not** be committed to those repositories.

Use a consistent scheme for directory trees
   For a submodule for a specific model variant or project named,
   for instance, :py:`message_ix_models.model.[name]` or :py:`message_ix_models.project.[name]`,
   keep input data in a well-organized directory under:

   - :file:`[base]/[name]/` —preferred, flatter,
   - :file:`[base]/model/[name]/`,
   - :file:`[base]/project/[name]/`,

   …or similar.

   Keep *project-specific configuration files* in the same locations:

   .. code-block:: python

      # Located in `message_ix_models/data/`:
      config = load_package_data("myproject", "config.yaml")

      # Located in `data/` in the message_data repo:
      config = load_private_data("myproject", "config.yaml")

      # Not recommended: located in the same directory as a code file
      config = yaml.safe_load(open(Path(__file__).with_name("config.yaml")))

   Use a similar scheme for output data, except under the :ref:`local data <local-data>` path.

Re-use configuration
   Configuration to run a set of scenarios or to prepare reported submissions **should** re-use or extend existing, general-purpose code.
   Do not duplicate code or configuration.
   Instead, adjust or selectively overwrite its behaviour via project-specific configuration read from a file.

.. _large-input-data:
.. _binary-input-data:

Large/binary input data
=======================

Large, binary input data, such as Microsoft Excel spreadsheets, **must not** be committed as ordinary Git objects.
This is because the entire file is re-added to the Git history for even small modifications,
making it very large (see `issue #37 <https://github.com/iiasa/message_data/issues/37>`_).

Instead, use one or more of the following patterns, in order of preference.
Whichever pattern is used, code for handling large input data **must** be in :mod:`message_ix_models`,
even if the data itself is private, for instance in :mod:`message_data` or another location.

Fetch directly from a remote source
-----------------------------------

This corresponds to section (1) above.
Preferably, do this via :mod:`message_ix_models.util.pooch`:

- Extend :data:`.pooch.SOURCE` to store the Internet location, file name(s), and hash(es) of the file(s).
- Call :func:`.pooch.fetch` to retrieve the file and cache it locally.
- Write code in :mod:`message_ix_models` that processes the data into a common format,
- for instance by subclassing :class:`.ExoDataSource`.

This pattern is preferred because it can be replicated by anyone, and the reference data is public.

This pattern may be applied to:

- Data published and maintained by others, or
- Data created by the IIASA ECE program to be used in :mod:`message_ix_models`,
  such as `Zenodo <https://zenodo.org>`_ records.

Use Git Large File Storage (LFS)
--------------------------------

`Git LFS <https://git-lfs.github.com/>`_ is a Git extension
that allows for storing large, binary files without bloating the commit history.
Essentially, Git stores a 3-line text file with a hash of the full file,
and the full file is stored separately.
The IIASA GitHub organization has up to 300 GB of space for such LFS objects.

To use this pattern, :program:`git add ...` and :program:`git commit` files in an appropriate location (above).
New or unusual binary file extensions may require a :program:`git lfs` command or modification to :file:`.gitattributes` to ensure they are tracked by LFS and not by Git itself.
See the Git LFS documentation for more detail.

For large files stored as :ref:`package data <package-data>` using Git LFS, these:

- **must** be added to :file:`MANIFEST.in`.
  This avoids including the files in distributions published on PyPI.
- **should** be added to :mod:`.util.pooch`.
  This allows users who install :mod:`message_ix_models` from PyPI to easily retrieve the data.
  This usage **must** be included in the documentation that describes the data files.

Retrieve data from existing databases
-------------------------------------

These include the same IIASA ECE Program :mod:`ixmp` databases that are used to store scenarios.
Documentation **must** be provided that ensures this data is reproducible:
that is, any original sources and code to create the database used by :mod:`message_ix_models`.

Other patterns
--------------

Some other patterns exist, but **should not** be repeated in new code,
and **should** be migrated to one of the above patterns.

- SQL queries against a Oracle/JDBC database.
  See :ref:`m-data:data-iea` (in :mod:`message_data`)
  and `issue #53 <https://github.com/iiasa/message_data/issues/53#issuecomment-669117393>`_
  for a description of how to replace/simplify this code.
