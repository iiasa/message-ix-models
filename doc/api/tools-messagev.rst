MESSAGE V input files
*********************

This document describes some of the file formats for the pre-:mod:`.ixmp` MESSAGE model, a.k.a. **MESSAGE V**, and code in :mod:`message_ix_models.tools.messagev` that reads these formats.

.. note:: See also the earlier :doc:`import_from_msgV_sqlite` for similar code/descriptions.

.. contents::
   :local:

``easemps3.free``: soft dynamic constraints
-------------------------------------------

Each constraint is specified by a single row with the following, space-separated entries:

1. **Constraint type.** `mpa` (constraint on activity) or `mpc` (constraint on capacity).
2. **Technology name.** Four-letter internal name/code of a technology, e.g. `uHap`.
3. **Lower/upper bound.** Either `LO` (decline constraint) or `UP` (growth constraint).
4. **Cost type.** One of:

   - `lev`: levelized costs.
   - `abs`: absolute costs.
   - `var`: variable costs.

5. **Growth rate for step 1.** Percentage points of growth/decline at which the constraint becomes active.

6. **Additional cost for step 1.** Additional cost applied to activity/capacity growth or decline beyond the rate in #5. Depending on #4, specified:

   - `lev`: in percentage points of the levelized cost of the technology.
   - `abs`, `var`: in absolute monetary value.

7. **Up to 4 additional pairs of 5 and 6.** Growth rates for successive constraints are cumulative.

An example:

.. code::

   mpa uEAp UP lev 5 50 15 300000

Here the constraint relates to a growth constraint (UP) for activities (mpa) and the technology for which the constraint is to be extended is uEAp.
The allowed rate of growth is increased by 5 %-points and each additional unit of output that can be produced costs 50 % of the levelized costs additional on top of the normal costs (i.e. the costs that result from building and using the additional capacity required for the additional production).

The second step increases the maximum growth rate further, by 15 %-points, but the costs are prohibitive (300000).

Soft constraints can be set for each technology individually. This can be done globally ("regions = all -glb") or for each region separately ("regions = cpa").


API reference
-------------

.. currentmodule:: message_ix_models.tools.messagev

.. automodule:: message_ix_models.tools.messagev
   :members:
