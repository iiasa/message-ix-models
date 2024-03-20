Quick-start guide to running a MESSAGEix-GLOBIOM baseline model
***************************************************************

.. contents::
   :local:

This starting guide is for researchers at IIASA or external collaborators, who are not constant developers of ``message_ix`` and related software.
It should support running scenarios of different models of the MESSAGEix-GLOBIOM family.

.. note:: The guide **does not** give guidelines on how to contribute in ``message_ix_models``.
          If you wish to contribute, please read and understand `Outline & organization <https://docs.messageix.org/en/latest/contributing.html>`_ completely.

.. note:: As this is just a quick guide, it is **very possible**, the following steps for running a scenario might **not work directly** on your machine.


Prerequisites
=============

The following prerequisites are necessary to be able to follow the steps of this guide and be able to run scenarios.
Most of these prerequisites are analogous to the prerequisites of running ``message_ix``.

Knowledge & skills
------------------

Please go through the `Prerequisite knowledge & skills <https://docs.messageix.org/en/stable/prereqs.html#prerequisite-knowledge-skills>`_ and check if you can tick the basic as well as the advanced usage.
Especially understanding the concept of virtual environments is important.
If needed, please educate yourself in the specific areas.

Going through this guide, some error messages might occur.
Most of them can be fixed by understanding the error message itself.
`Understanding the Python Traceback <https://realpython.com/python-traceback>`_ might support with reading and interpreting an error message in Python.
If an error message is not directly understandable, google-ing it is often very helpful.
`Stack overflow <https://stackoverflow.com>`_ is a great source of support, and most likely someone else already had the same question on how to solve an error.

If you couldn't find a solution on the internet, please `open an issue on the main repository <https://github.com/iiasa/message-ix-models/issues/new>`_ or another repository of the ``message_ix`` software stack, as appropriate.

.. tip:: If you run into issues related to the setup, please also check `Common issues <https://docs.messageix.org/en/stable/install.html#common-issues>`_.


Installation
============

Go through the installation process of ``message-ix-models`` via :ref:`installation`.


Preparation
===========

To use MESSAGEix-GLOBIOM model tools, you need to store and access data in a database (DB). :mod:`ixmp` provides this service via so-called platforms.
To receive the data for a global model, you can either configure access to an existing DB or download the snapshot from Zenodo.


Configure access to existing DBs
--------------------------------

.. note:: The existing DBs at IIASA facilities are not open to the public. If you are interested in collaborating, please reach out to the ``message_ix`` modelling team.

Every model in ``message-ix-models`` should contain information about the needed platform.
``ixmp`` keeps track of these platforms.

Use the ``ixmp`` CLI to add a specific platform::

    $ ixmp platform add <PLATFORMNAME> jdbc oracle x8oda.iiasa.ac.at:1521/<PATH> <USERNAME> <PASSWORD>

``PATH`` will be ``pIXMP2.iiasa.ac.at`` for most modern DBs, but could also be ``pIXMP1.iiasa.ac.at`` on older instances.

.. note:: If you are a collaborator, but do not know the ``USERNAME`` and/or ``PASSWORD`` of the platform you want to use, please reach out to the ``message_ix`` community manager.

If you do not have access to the IIASA DBs, you need to configure a local DB. 
For small applications, you can replace the URL ``x8oda.iiasa.ac.at:1521/pIXMP2.iiasa.ac.at`` with a path on your system and omit ``USERNAME`` and ``PASSWORD``.
For more complex modelling needs and required infrastructure, please reach out to the ``message_ix`` modelling team. 

Please see the `Command-line interface documentation <https://docs.messageix.org/projects/models/en/latest/cli.html>`_ of ``message-ix-models`` for further information, e.g. how to set a platform as default.


Download snapshot from Zenodo
-----------------------------

The latest version of a MESSAGEix-GLOBIOM baseline model can be found `on Zenodo <https://zenodo.org/doi/10.5281/zenodo.5793869>`_. 
After downloading this archive, you can extract the data file, which is in ``xlsx`` format. 
You can then pass this file to :meth:`~ixmp.Scenario.read_excel` to read it into a |Scenario| and store it in a local DB via :meth:`~ixmp.TimeSeries.commit`.
For your convenience, you can also ``fetch`` the snapshot from the CLI and ``load`` it in your code as described in :ref:`model-snapshot`.




