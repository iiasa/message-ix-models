Building models (:mod:`.model.build`)
*************************************

:func:`.apply_spec` can be used to be build (compose, assemble, construct, …) models given three pieces of information:

- A :class:`.message_ix.Scenario` to be used as a base.
  This scenario may be empty.
- A specification, or :class:`.Spec`, which is trio of :class:`.ScenarioInfo` objects; see below.
- An optional function that adds or produces `data` to add to the `scenario`.

The spec is applied as follows:

1. For each :mod:`ixmp` set that exists in `scenario`:

   a. **Required** elements from :attr:`.Spec.require`, if any, are checked.

      If they are missing, :func:`apply_spec` raises :class:`ValueError`.
      This indicates that `spec` is not compatible with the given `scenario`.

   b. Elements from :attr:`.Spec.remove`, if any, are **removed**.

      Any parameter values which reference these set elements are also removed, using :func:`.strip_par_data`.

   c. New set elements from :attr:`.Spec.add`, if any, are **added**.

2. Elements in ``spec.add.set["unit"]`` are added to the Platform on which
   `scenario` is stored.

3. The `data` argument, a function, is called with `scenario` as the first argument, and a keyword argument `dry_run` from :func:`.apply_spec`.
   `data` may either add to `scenario` directly (by calling :meth:`.Scenario.add_par` and similar methods); or it can return a :class:`dict` that can be passed to :func:`.add_par_data`.


The following modules use this workflow and can be examples for developing similar code:

- :mod:`.model.bare`
- :mod:`.model.disutility`
- :mod:`message_data.model.transport`


Code reference
==============

.. currentmodule:: message_ix_models.model.build

.. automodule:: message_ix_models.model.build
   :members:
