.. currentmodule:: message_ix_models.tools.ict

ICT sector demand projections (:mod:`.tools.ict`)
*************************************************

:mod:`.tools.ict` implements the methods of Fan et al. (2026) :cite:`fan_2026_data_center_energy` to **project future electricity demand by ICT equipment** and provides functionality to represent them explicitly in MESSAGEix-GLOBIOM family models.

.. contents::
   :local:

.. note:: Prototypes that generate prior versions of ICT demand projections can be found on a `project branch <https://github.com/iiasa/message-ix-models/tree/project/digsy>`_, which was used in :mod:`project.prisma` and :mod:`project.digsy`.

Methods
=======

The tool creates projected energy demand values for different regions and scenarios.
The projections are calibrated on historical data and projected socio-economic development.
The projections reproduce the results of Fan et al. (2026), which end at 2050.
Since MESSAGEix-GLOBIOM family models often run until 2100, the tool can also extend the projections to 2100.

Besides the temporal extrapolation, the tool also provides functionality to disaggregate the projections from the 5 regions used by Fan et al. (2026).
The current disaggregation method uses the number of data centers in 2025 per country as reported by `cloudscene.com <https://cloudscene.com>`_.

Extrapolation beyond 2050
-------------------------

The method used to further extend the projections is similar to the 2030-2025 extrapolation method used by Fan et al. (2026).
The data sample used to estimate the long term elasticity between digital transformation and data center energy is 2010 and the 2030 IEA Base scenario projection.
Fan et al. (2025) provides digital transformation index projections until 2100, which are used by the tool to drive the extrapolation.
In contrast to the 2030-2025 extrapolation, the 2100 extrapolation omits regional residuals and uses the global average elasticity to project the energy demand of each region.

.. note:: The extrapolation past 2050 should be considered as only exploratory scenarios given the high level of uncertainty around data centre demand growth.
 In contrast to the 2050 projections, this projection has not been peer-reviewed and should be used with caution.

Tele communication equipment
----------------------------

To cover the ICT sector as comprehensively as possible, the tool also provides projections for telecommunication equipment energy demand.
As Fan et al. (2026) focuses on data centers this not explicitly projected.
The tool uses the range of historically observed ratios between data center and telecommunication equipment energy demand.
The ratio is then simply used to scale the projected data center energy demand to obtain the telecommunication equipment energy demand.

Model structure
---------------

Besides the demand projections, the tool can also build a MESSAGEix-GLOBIOM model structure to integrate the projections.
For this purpose, a simple technology structure has been implemented, consisting of two technologies:

- one for data centers
- one for telecommunication equipment

These technologies are only parametrized for two parameters: fixed_activity and input.
Since electricity consumption is the input of interest here, the input commodity is set to the MESSAGEix-GLOBIOM internal electricity code `electr`.
fixed_activity is parametrized using the projected energy demand values.

Data sources
============

The tool uses the following data sources:

- IEA: Energy and AI (2025) :cite:`iea_2025_ai_energy`
- IEA: Key Questions on Energy and AI (2026) :cite:`iea_2026_ai_energy`
- Fan et al. (2025). "Digital transformation in the Shared Socioeconomic Pathways". :cite:`fan_2025_ssp_digital_transformation`
- Fan et al. (2026). "Data centre energy demand projections within shared socioeconomic pathways". :cite:`fan_2026_data_center_energy`

The tool also uses :mod:`.ssp.data` (via :func:`.exo_data.prepare_computer`) to compute digital transformation index for multi-country regions by taking population-weighted averages.

Usage
=====

:func:`.build` is the top-level entry point.

This function takes :class:`.Context` object and :class:`.Scenario` as arguments.
The Context needs to contain a :class:`.Config` with all the settings understood by the module.
Those settings include the following; click each for the full description, allowable values, and defaults:

   :attr:`~.Config.ssp`,
   :attr:`~.Config.scenario`,
   :attr:`~.Config.lastyear`,
   :attr:`~.Config.tele_comm_ratio`,

To use the tool, you **must** first obtain a copy of the SSP input data.
See the documentation at :mod:`message_ix_models.project.ssp.data`.

Next, create a :class:`.Config` object and extend the :class:`.Context` object::

   from message_ix_models.tools.ict import build, Config

   # Use default settings
   cfg = Config()

   # Build the ICT model structure and projections on a given `message_ix.Scenario`
   build(context, scenario)

Code reference
==============

The top-level function and configuration class:

.. autosummary::

   Config
   build

The other submodules implement the supporting methods, calculations, and data handling:

1. :mod:`~.ict.digtrans` reads the projected digital transformation index of Fan et al., 2025.
2. :mod:`~.ict.iea` reads historical and projected energy demand up to 2030 of the IEA.
3. :mod:`~.ict.data_center` reads the supplementary materials of Fan et al., 2026.
4. :mod:`~.ict.data` combines the data from the previous three submodules to generate demand projections and build the model structure.

.. automodule:: message_ix_models.tools.ict
   :members:
