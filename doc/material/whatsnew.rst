.. _whatsnew_0100:

Version 1.1.0 (August 27, 2024)
----------------------------------

{{ header }}


This is a release from MESSAGEix-Materials 1.0.0 and includes model additions and enhancements.
There are also a few of changes in the command line interface.

Model changes
~~~~~~~~~~~~~~~~~~~~~~~~~
**Iron & steel sector**
This release introduces the following decarbonization technologies:

- (list technologies here)

Shared input data file :file:`Global_steel_cement_MESSAGE.xlsx` has been separated for steel and cement data.

**Non metallic minerals sector**

Shared input data file has been separated for steel and cement data.

CCS addon reformulated to prohibit wet CCS addon on dry clinker kiln and vice versa.

**Non ferrous metals sector**

Global trade of aluminum calibrated according to IAI MFA model.

**Power sector**

Material intensity of hydro power plants updated (reference?)

**Demand generator**

GDP and population projections are read from scenarios by default.
If the data is not available, default projections are used.

**Other**

MESSAGEix-Materials can be built on the :ref:`model-bare` now.

Convenience function :func:`data_util.calculate_ini_new_cap` to calculate ``initial_new_capacity_up/lo`` based on demand parameter.

MACRO calibration is now available for non-IIASA users who did not have access to the previously required MACRO calibration file.
See :ref:`CLI Changes`

An option has been added to shift the first model year of a scenario to a desired year before solving.
See :ref:`CLI Changes`


CLI changes
~~~~~~~~~~~

The :func:`build_scen` command uses the ``mix-models`` CLI parameter ``--nodes`` now.
Currently ``R12`` is the only supported option.

Scenarios can now be calibrated with MACRO by setting ``--add_calibration=True`` and setting ``--macro-file`` to an existing filename located in :file:`data/material/macro/`.

Solving a scenario with a different first model year is possible with e.g.::

   mix-models --url="ixmp://ixmp_dev/MESSAGEix-Materials/scenario_name" material-ix \
     SSP2 solve --shift_model_year=2025

Depreciations
~~~~~~~~~~~
The ``material-ix`` commands ``add-buildings-ts``, ``report-2`` and ``create-bare`` have been removed.

New features
~~~~~~~~~~~~

**Bug Fixes**


**Compatibility**
