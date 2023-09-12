Distributed computing
*********************

This page introduces considerations, tools, and features for using **distributed** or **high-throughput computing** with MESSAGEix-GLOBIOM.

.. contents::
   :local:
   :backlinks: none

Overview
========

Scenarios in the MESSAGEix-GLOBIOM global model family are characterized by:

- ca. 100 MB of data, depending on storage format (e.g. in a :class:`~ixmp.backend.jdbc.JDBCBackend` local, HyperSQL database, or :ref:`ixmp:excel-data-format` in Excel files).
- :meth:`~message_ix.Scenario.solve` times of between 10 and 60 minutes, depending on hardware and configuration, plus similar amounts of time to run the legacy reporting in :mod:`message_data`.
- Memory usage of ~10 GB or more using :class:`~ixmp.backend.jdbc.JDBCBackend`, currently the only supported backend.

These resource needs can be a bottleneck in applications, for example:

- where many/related scenarios must be solved.
- when iteration (repeatedly solving 1 or more scenarios) is a key approach in developing code that sets up scenarios.

To improve research productivity, researchers may choose to run scenarios or ‘workflows’ (a combination of solving scenarios and pre- and post-processing steps or codes) through **distributed computing**, i.e. not on their local machine.
Hardware and software environments for distributed computing can vary widely, and can be categorized in multiple ways, such as:

1. More powerful single-CPU systems, accessed remotely.
2. Cloud services, e.g. Google Compute Engine; Amazon AWS; Github Actions; etc. providing access to one or more machines.
3. Dedicated cluster systems (sometimes labelled **high-throughput computing**, HTC, or **high-performance computing**, HPC, systems) for scientific computing, operated by a variety of parties.

Tooling
=======

- :mod:`message_ix_models` and related packages (:mod:`ixmp`, :mod:`message_ix`, :mod:`message_data`) aim to provide *simple, general-purpose features* that allow working with a variety of distributed systems.
- Specific configuration necessarily depends on the specific system(s) in use and the researcher's application.
- These features should not duplicate features of existing tools such as
  `Slurm <https://slurm.schedmd.com>`_,
  `HTCondor <https://htcondor.readthedocs.io/en/latest/>`_, etc., but rather allow :mod:`message_ix` & co. to be used with/through those.
- The individual features, tools, utilities, etc. should each be simple, i.e. `do one thing, and do it well <https://en.wikipedia.org/wiki/Unix_philosophy#Do_One_Thing_and_Do_It_Well>`__.

.. todo:: Extend.
