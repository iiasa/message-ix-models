.. _year-yaml:

Years or time periods (:file:`year/*.yaml`)
*******************************************

- See also:

  - The discussion of :doc:`message-ix:time` in the :mod:`message_ix` documentation, which explains the standard sense of time periods used across the MESSAGEix framework and specific models based on it.
  - :meth:`.ScenarioInfo.year_from_codes`

- These are not the only possible meanings of these codes; others may be used in data from other sources.

  For instance, the ID ``2020`` could be used to represent the period from 2017-07-01 to 2022-06-30.
  These lists alone cannot resolve these differences; they exist only to provide clarity about the sense used in :mod:`message_ix_models`.

- When working with data from other sources, :mod:`message_ix_models` code **must**:

  - Explicitly note (e.g. in comments or docstrings) any differing time discretization used in the other data.
  - Perform appropriate conversion *or* record a decision to use the data directly, without conversion.

- It is **optional** for code to fill Scenario parameters for the full set of historical years.

  For instance, when working with list ``B``, code for a model variant or project could only populate parameter values for the historical periods ``2010`` and ``2015``, but not ``2005`` and earlier.
  This **should** be described and documented on at the scope (function or module) where such subsets are selected from the full codelist.

List ``A``
----------

.. literalinclude:: ../../message_ix_models/data/year/A.yaml
   :language: yaml

List ``B``
----------

.. literalinclude:: ../../message_ix_models/data/year/B.yaml
   :language: yaml
