.. currentmodule:: message_ix_models.model.disutility

Consumer disutility (:mod:`model.disutility`)
*********************************************

This module provides a generalized consumer disutility formulation, currently used by :mod:`message_data.model.transport`.
The formulation rests on the concept of “consumer groups”; each consumer group may have a distinct disutility associated with using the outputs of each technology.
A set of ‘pseudo-’/‘virtual’/non-physical “usage technologies” converts the outputs of the actual technologies into the commodities demanded by each group, while also requiring input of a costly “disutility” commodity.


Method & usage
==============

Use this code by calling :func:`add`, which takes arguments that describe the concrete usage:

Consumer groups
   This is a list of |Code| objects describing the consumer groups.
   The list must be 1-dimensional, but can be composed (as in :mod:`message_data.model.transport`) from multiple dimensions.

Technologies
   This is a list of |Code| objects describing the technologies for which the consumers in the different groups experience disutility.
   Each object must be have 'input' and 'output' annotations (:attr:`~.Code.annotations`); each of these is a :class:`dict` with the keys 'commodity', 'input', and 'unit', describing the source or sink for the technology.

Template
   This is also a |Code| object, similar to those in ``technologies``; see below.

The code creates a source technology for the “disutility” commodity.
The code does *not* perform the following step(s) needed to completely parametrize the formulation:

- Set consumer group-specific ``demand`` parameter values for new commodities.
- Set the amounts of “disutility” commodities used as ``input`` to the new usage technologies.

These must be parametrized based on the particular application.

Detailed example
================

This example is similar to the one used in :func:`.test_disutility.test_minimal`:

.. code-block:: python

    # Two consumer groups
    groups = [Code(id="g0"), Code(id="g1")]

    # Two technologies, for which groups may have different disutilities.
    techs = [Code(id="t0"), Code(id="t1")]

    # Add generalized disutility formulation to some technologies
    disutility.add(
        scenario,
        groups=groups,
        technologies=techs,

        template=Code(
            # Template for IDs of conversion technologies
            id="usage of {technology} by {group}",

            # Templates for inputs of conversion technologies
            input=dict(
                # Technology-specific output commodity
                commodity="output of {technology}",
                level="useful",
                unit="kg",
            ),

            # Templates for outputs of conversion technologies
            output=dict(
                # Consumer-group–specific demand commodity
                commodity="demand of group {group}",
                level="useful",
                unit="kg",
            ),
        ),
        **options,
    )


:func:`add` uses :func:`get_spec` to generate a specification that adds the following:

- For the set ``commodity``:

  - The single element “disutility”.
  - One element per `technologies`, using the `template` “input” annotation, e.g. “output of t0” generated from ``output of {technology}`` and the id “t0”.
    These **may** already be present in the `scenario`; if not, the spec causes them to be added.
  - One elements per `groups`, using the `template` “output” annotation, e.g. “demand of group g1” generated from ``demand of group {group}`` and the id “g1”.
    These **may** already be present in the `scenario`; if not, the spec causes them to be added.

- For the set ``technology``:

  - The single element “disutility source”.
  - One element per each combination of disutility-affected technology (`technologies`) and consumer group (`groups`).
    For example, “usage of t0 by g1” generated from ``usage of {technology} by {group}``, and the ids “t0” and “g1”.

The spec is applied to the target scenario using :func:`.model.build.apply_spec`.
If the arguments produce a spec that is inconsistent with the target scenario, an exception will by raised at this point.


Next, :func:`add` uses :func:`data_conversion` and :func:`data_source` to generate:

- ``output`` and ``var_cost`` parameter data for “disutility source”.
  This technology outputs the unitless commodity “disutility” at a cost of 1.0 per unit.

- ``input`` and ``output`` parameter data for the new usage technologies.
  For example, the new technology “usage of t0 by g1”…

  - …takes input from the *technology-specific* commodity “output of t0”.
  - …takes input from the common commodity “disutility”, in an amount specific to group “g1”.
  - …outputs to a *group-specific* commodity “demand of group g1”.

Note that the `technologies` towards which the groups have disutility are assumed to already be configured to ``output`` to the corresponding commodities.
For example, the technology “t0” outputs to the commodity “output of t0”; the ``output`` values for this technology are **not** added/introduced by :func:`add`.

.. _disutility-units:

(Dis)utility is generally dimensionless.
In :mod:`.pint` and thus also :mod:`message_ix_models`, this should be represented by ``""``.
However, to work around `iiasa/ixmp#425 <https://github.com/iiasa/ixmp/issues/425>`__, :func:`data_conversion` and :func:`data_source` return data with ``"-"`` as units.
See :issue:`45` for more information.

Code reference
==============

See also :mod:`message_ix_models.tests.model.test_disutility`.

.. automodule:: message_ix_models.model.disutility
   :members:
