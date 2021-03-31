.. currentmodule:: message_data.model.disutility

Consumer disutility
*******************

This module provides a generalized consumer disutility formulation, currently used by :mod:`message_data.model.transport`.

The formulation rests on the concept of “consumer groups.”
Each consumer group may have a distinct disutility for using the outputs of each technology.


Method & usage
==============

Use this code by calling :func:`add`, which takes arguments that describe the concrete usage:

Consumer groups
   This is a list of :class:`.Code` objects describing the consumer groups.
   The list must be 1-dimensional, but can be composed (as in :mod:`message_data.model.transport`) from multiple dimensions.

Technologies
   This is a list of :class:`.Code` objects describing the technologies for which the consumers in the different groups experience disutility.
   Each object must be have 'input' and 'output' annotations (:attr:`.Code.anno`); each of these is a :class:`dict` with the keys 'commodity', 'input', and 'unit', describing the source or sink for the technology.

Template
   This is also a :class:`.Code` object, similar to those in ``technologies``; see below.


The code does *not* do the following steps needed to completely parametrize the formulation:

- Set consumer group-specific 'demand' parameter values for new commodities.
- Create a source technology for the “disutility” commodity.


Detailed example
================

From :func:`.transport.build.main`:

.. code-block:: python

    # Add generalized disutility formulation to LDV technologies
    disutility.add(
        scenario,

        # Generate a list of consumer groups
        consumer_groups=consumer_groups(),

        # Generate a list of technologies
        technologies=generate_set_elements("technology", "LDV"),

        template=Code(
            # Template for IDs of conversion technologies
            id="transport {technology} usage",

            # Templates for inputs of conversion technologies
            input=dict(
                # Technology-specific output commodity
                commodity="transport vehicle {technology}",
                level="useful",
                unit="km",
            ),

            # Templates for outputs of conversion technologies
            output=dict(
                # Consumer-group–specific demand commodity
                commodity="transport pax {mode}",
                level="useful",
                unit="km",
            ),
        ),
        **options,
    )


:func:`add` uses :func:`get_spec` to generate a specification that adds the following:

- A single 'commodity' set element, “disutility”.

- 1 'mode' set element per element in ``consumer_groups``.

  **Example:** the function :func:`.consumer_groups` returns codes like “RUEAA”, “URLMF”, etc.; one 'mode' is created for each such group.

- 1 'commodity' set element per technology in ``technologies``.
  ``template.anno["input"]["commodity"]`` is used to generate the IDs of these commodities.

  **Example:** “transport vehicle {technology}” is used to generate a commodity “transport vehicles ELC_100” associated with the technology with the ID “ELC_100”.

- 1 'commodity' set element per consumer group.
  ``template.anno["output"]["commodity"]`` is used to generate the IDs of these commodities.

  **Example:** “transport pax {mode}” is used with to generate a commodity “transport pax RUEAA” is associated with the consumer group with ID “RUEAA”.

- 1 additional 'technology' set element per disutility-affected technology.
  ``template.id`` is used to generate the IDs of these technologies.

  **Example:** “transport {technology} usage}” is used to generate “transport ELC_100 usage” associated with the existing technology “ELC_100”.


The spec is applied to the target scenario using :func:`.model.build.apply_spec`.
If the arguments produce a spec that is inconsistent with the target scenario, an exception will by raised at this point.


Next, :func:`add` uses :func:`disutility_conversion` to generate data for the 'input' and 'output' parameters, as follows:

- Existing, disutility-affected technologies (those listed in the ``technologies`` argument) 'output' to technology-specific commodities.

  **Example:** the technology “ELC_100” outputs to the commodity “transport vehicle ELC_100”, instead of to a common/pooled commodity such as “transport vehicle”.

- New, conversion technologies have one 'mode' per consumer group.

  **Example:** the new technology “transport ELC_100 usage”

  - …in “all” modes—takes the *same* quantity of input from the *technology-specific* commodity “transport ELC_100 vehicle”.
  - …in each consumer-group specific mode e.g. “RUEAA”—takes a *group-specific* quantity of input from the common commodity “disutility”.
  - …in each consumer-group specific mode e.g. “RUEAA”—outputs to a *group-specific* commodity, e.g. “transport pax RUEAA”.


Code reference
==============

.. automodule:: message_ix_models.model.disutility
   :members:
