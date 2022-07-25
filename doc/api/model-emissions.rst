Emissions data (:mod:`.model.emissions`)
========================================

.. currentmodule:: message_ix_models.model.emissions

:mod:`.model.emissions` contains codes for working with emissions data, including policies on emissions.

In general, models created with :mod:`message_ix_models`:

- Use tonnes of carbon equivalent ("t C") as units for mass of emissions.
- Use "USD / t C" as units for price of emissions.
  Because (as of 2022-07-20) :mod:`iam_units` treats "USD" as an alias for "USD_2005", this is implicitly USD_2005 / t C.

.. automodule:: message_ix_models.model.emissions
   :members:
