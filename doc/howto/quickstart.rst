Run a MESSAGEix-GLOBIOM baseline model
**************************************

.. contents::
   :local:

This starting guide is for researchers at IIASA or external collaborators, who are not constant developers of :mod:`message_ix` and related software.
It should support running scenarios of different models of the MESSAGEix-GLOBIOM family.

.. note::
   - This is only a guide, based on some users' experiences.
     It is **very possible** that the following steps for running a scenario **will not work directly** on your machine.
   - This guide **does not** cover how to contribute to :mod:`message_ix_models`.
     If you wish to contribute, please read and understand our :doc:`contribution guide <message-ix:contributing>` completely.

Prerequisites
=============

The following prerequisites are necessary to be able to follow the steps of this guide and be able to run scenarios.
Most of these prerequisites are analogous to the prerequisites of using :mod:`message_ix`.

Knowledge & skills
------------------

Please go through the :doc:`message-ix:prereqs` in the :mod:`message_ix` documentation.
You should understand **all** of the basic pre-requisites, and **most** of the advanced pre-requisites.
Understanding the concept of virtual environments is especially important.
If needed, please educate yourself in the specific areas.

Going through this guide, some error messages might occur.
Most of them can be fixed by understanding the error message itself.
`Understanding the Python Traceback <https://realpython.com/python-traceback>`_ might support with reading and interpreting an error message in Python.
If an error message is not directly understandable, a web search is often very helpful.
`Stack Overflow <https://stackoverflow.com>`_ is a great source of support, and most likely someone else already had the same question on how to solve an error.

If you couldn't find a solution on the internet, please `open an issue on the main repository <https://github.com/iiasa/message-ix-models/issues/new>`_ or another repository of the :mod:`message_ix` software stack, as appropriate.

Installation
============

Go through the installation process of :mod:`message_ix_models` via :doc:`install`.

.. tip:: If you run into issues related to installing :mod:`message_ix`, please also check :ref:`Common issues <message-ix:common-issues>` in its installation docs.

Preparation
===========

When using :mod:`message_ix_models`, you will store and access data for distinct scenarios.
:mod:`ixmp` provides this storage service via :class:`Platforms <Platform>`.
Each platform corresponds to a database (DB), either local to your system or remote.
To receive the data for a global model, you can either configure access to an existing, remote DB; or download a model ‘snapshot’ file from Zenodo and load it into your local DB.


Configure access to existing DBs
--------------------------------

.. note:: The existing DBs at IIASA facilities are not publicly accessible.
   If you are interested in collaborating, please reach out to the |MESSAGEix| team.

Every model variant and project documented with :mod:`message_ix_models` and :mod:`message_data` should contain information about the platform where associated scenarios are stored.
(If not, reach out to the person(s) responsible for those variants/projects and ask them.)
:mod:`ixmp` keeps track of these platforms.
See the :mod:`message_ix_models` :doc:`cli`, and the :mod:`ixmp` :ref:`ixmp:configuration` documentation for how to configure these. [1]_
To access the IIASA DBs:

- ``<Computer>`` is ``x8oda.iiasa.ac.at``,
- ``<PORT>`` is ``1521``,
- ``<PATH>`` will be ``pIXMP2.iiasa.ac.at`` for most modern DBs, but could also be ``pIXMP1.iiasa.ac.at`` on older instances.

.. note:: If you are a collaborator, but do not know the ``<USERNAME>`` and/or ``<PASSWORD>`` of the platform you want to use, please reach out to the |MESSAGEix| Community Manager, currently :gh-user:`glatterf42`.

.. [1] The :program:`mix-models config ...` CLI is identical to the :program:`ixmp config ...` CLI, and has exactly the same behaviour.

If you do not have access to the IIASA DBs, you need to configure a local DB.
This is appropriate for small applications (i.e. less than hundreds of scenarios).
Per the ixmp “Configuration” docs linked above, in your call to :program:`mix-models config ...` you can:

- replace ``oracle`` with ``hsqldb``,
- replace the URL ``<COMPUTER>:<PORT>/<PATH>`` with a path on your system, and
- omit ``<USERNAME>`` and ``<PASSWORD>``.

For more complex modelling needs and required infrastructure, please reach out to the |MESSAGEix| team.

Download snapshot from Zenodo
-----------------------------

The latest version of a MESSAGEix-GLOBIOM baseline model can be found `on Zenodo <https://zenodo.org/doi/10.5281/zenodo.5793869>`_.
For convenience, the function :func:`.snapshot.load` (documented at :ref:`model-snapshot`) automates the following steps, but you can also perform them manually:

- Fetch the snapshot from Zenodo.
  To do this manually, do one of the following:

  - Use a CLI command like::

      mix-models fetch snapshot-1

  - Call :func:`.pooch.fetch`.
  - Download it from Zenodo via your browser and extract the :file:`.xlsx` data file from the :file:`.zip` archive.

- Pass the file to :meth:`~Scenario.read_excel` to read it into a :class:`~message_ix.Scenario` and store it in a local DB via :meth:`~Scenario.commit`.
