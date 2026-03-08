BMT workflow
************

**BMT** (Buildings, Materials, Transport) is a workflow that chains the
MESSAGEix-GLOBIOM **B**uildings, **M**aterials, and **T**ransport variants
into a single run: load a base scenario, add sectoral structure and data,
solve, report, add policies/emission budgets, and include dynamic price-demand feedback.

It is implemented in :mod:`.model.bmt.workflow` and used via the :program:`bmt`
CLI subcommand.

Workflow steps
==============

:func:`.model.bmt.workflow.generate` returns a :class:`.Workflow` whose steps
include:

- **M** — load base MESSAGE scenario
- **M cloned** — clone to a BMT model/scenario name
- #TODO: to be filled later

Configuration
=============

All BMT workflow configuration is read from :file:`data/bmt/config.yaml`.
The YAML has top-level sections per sector (e.g. ``buildings``, ``transport``,
``materials``, and others); each section is loaded and attached to the corresponding
context key.

Usage
=====

Example (dry run)::

  mix-models bmt run --from="base" "glasgow+" --dry-run

See :program:`mix-models bmt run --help` for options.

See also
========

- :doc:`/buildings/index` — MESSAGEix-Buildings
- :doc:`/material/index` — MESSAGEix-Materials
- :doc:`/transport/index` — MESSAGEix-Transport
