.. currentmodule:: message_ix_models.project.alps

ALPS (:mod:`.project.alps`)
***************************

ALPS (ALternative Pathways toward Sustainable development) injects climate impact drivers (CIDs) into MESSAGE-GLOBIOM scenarios.
The pipeline translates MESSAGE emissions through climate modeling to basin-level hydrological and regional energy impacts:

.. code-block:: text

   MESSAGE scenario
       -> emissions reporting
   IAMC-format emissions
        -> MAGICC climate model
   100 GMT trajectories
       -> RIME emulators
   Basin/regional CID predictions
       -> scenario_generator
   MESSAGE scenarios with CID parameters

Three CID types are implemented: water availability, thermoelectric cooling capacity, and building energy demand.

Note : CID scenarios are not emission constrained directly. Instead we use RIME derived CIDs from emission constrained scenarios ("1100f", "2350f").
This is to isolate the pure climate impact from policy effects like carbon budgets.

.. contents::
   :local:
   :depth: 2

CID Variables
=============

Water Availability
------------------

Water CIDs modify basin-level water supply parameters based on RIME hydrological emulators.

Two RIME variables are used:

- ``qtot_mean``: Total runoff (km³/yr per basin). Mapped to ``surfacewater_basin`` demand.
- ``qr``: Groundwater recharge (km³/yr per basin). Mapped to ``groundwater_basin`` demand and the ``gw_share`` constraint.

The following MESSAGE parameters are modified:

.. list-table::
   :header-rows: 1
   :widths: 25 25 25 25

   * - Parameter
     - Commodity/Relation
     - Dimensions
     - Unit
   * - ``demand``
     - ``surfacewater_basin``
     - node, year, time
     - MCM/year
   * - ``demand``
     - ``groundwater_basin``
     - node, year, time
     - MCM/year
   * - ``share_commodity_lo``
     - ``share_low_lim_GWat``
     - node_share, year_act, time
     - dimensionless

RIME outputs km³/yr; MESSAGE expects MCM/year.
Conversion factor is 1000.
Values are negated following the MESSAGE demand convention (negative = supply).

The groundwater share is computed as:

.. math::

   \text{gw\_share} = 0.95 \times \frac{q_r}{q_{tot} + q_r}

capped at [0, 1].

Basin Geometry
~~~~~~~~~~~~~~

RIME emulators predict at 157 unique basins.
MESSAGE R12 has 217 basin-region rows because some basins span multiple R12 regions.
The :func:`.rime.split_basin_macroregion` function expands predictions via area-weighted splitting:

.. math::

   V_{\text{message}}[i] = V_{\text{rime}}[\text{basin\_id}] \times \frac{A_{\text{fragment}}[i]}{A_{\text{basin\_total}}}

Three basins have nan values from the RIME processing pipeling.
They are basins 0, 141, and 154; original scenario values are preserved for these basins.

Seasonal Transformation
~~~~~~~~~~~~~~~~~~~~~~~

The transformation is equivalent to having monthly timeseries per basin and aggregating into two half-year periods.
RIME seasonal emulators were developed first to analyze per-basin seasonality (dry/wet regimes vary by hemisphere and local climate).
MESSAGE computational constraints then required aggregation to two timeslices: h1 (Jan-Jun) and h2 (Jul-Dec).

The file :file:`joint_bifurcation_mapping_CWatM_2step.csv` specifies which months belong to dry vs wet season for each basin.
A transformation matrix redistributes seasonal rates to fixed timeslices based on month overlap:

.. math::

   \begin{bmatrix} R_{h1} \\ R_{h2} \end{bmatrix} = \mathbf{T} \begin{bmatrix} R_{dry} \\ R_{wet} \end{bmatrix}

where :math:`\mathbf{T}_{ij}` is the fraction of season :math:`j` months falling in timeslice :math:`i`, divided by 6.
Annual volume is preserved since each column sums to the season's share of the year.

Thermoelectric Capacity Factor
------------------------------

Warming reduces thermoelectric power plant efficiency by constraining heat rejection to cooling water.
This follows the methodology of Jones et al. (2025) :cite:`jones-2025`.

.. note::

   Reference: Jones et al. (2025), *Environmental Research: Water*, DOI: `10.1088/3033-4942/addffa <https://doi.org/10.1088/3033-4942/addffa>`_

Power plant cooling efficiency depends on the temperature differential between cooling water discharge limit and river intake temperature:

.. math::

   \Delta T_{\text{avail}} = T_{l,\text{max}} - T_{\text{river}}

where :math:`T_{l,\text{max}}` is the maximum permissible discharge temperature (95th percentile of historical daily water temperature) and :math:`T_{\text{river}}` is simulated river water temperature.

As climate warms, :math:`T_{\text{river}}` rises, shrinking :math:`\Delta T_{\text{avail}}`.
Plants must curtail generation when they cannot reject heat without exceeding thermal pollution limits.

The capacity factor dataset :file:`r12_capacity_gwl_ensemble.nc` was produced through a 4-stage pipeline:

1. Distributional downscaling from 86 countries to R12 regions using LOWESS/isotonic regression
2. Variance restoration via block bootstrap with coverage-based shrinkage
3. Ensemble generation (50 realizations × 15 GCM-SSP combinations)
4. GWL binning at :math:`0.1°\text{C}` increments

Capacity factors become ``relation_activity`` constraints that bound freshwater cooling technology activity relative to parent power plant activity:

.. math::

   \sum_{t \in \text{fresh\_cooling}} \text{ACT}[t] - r_{\text{jones}} \times s_{\text{ref}} \times f_{\text{cool}} \times \text{ACT}[\text{parent}] \leq 0

where:

- :math:`r_{\text{jones}} = \text{CF}(\text{GMT}) / \text{CF}_{\text{baseline}}` is the Jones ratio (capacity factor relative to baseline GWL)
- :math:`s_{\text{ref}}` is baseline freshwater cooling share from :file:`cooltech_cost_and_shares_ssp_msg_R12.csv`
- :math:`f_{\text{cool}}` is cooling energy per unit electricity from ``addon_conversion``

All thermoelectric parents with freshwater cooling variants are constrained (``coal_ppl``, ``gas_cc``, ``nuc_lc``, etc.).
The constraint applies to ``__cl_fresh`` (closed-loop) and ``__ot_fresh`` (once-through) variants.
Saline and air cooling are unconstrained, allowing the model to shift cooling technology mix under warming.

The following MESSAGE parameters are modified:

.. list-table::
   :header-rows: 1
   :widths: 30 40 30

   * - Parameter
     - Content
     - Value
   * - ``relation``
     - ``fresh_cool_bound_{parent}``
     - (set membership)
   * - ``relation_activity``
     - Coefficients for cooling techs (+1) and parent (negative)
     - varies by region/year
   * - ``relation_upper``
     - Upper bound on constraint
     - 0.0

Building Energy Intensity
-------------------------

Building CIDs modify space cooling (``rc_spec``) and heating (``rc_therm``) demands based on RIME energy intensity emulators.
These emulators are derived from the CHILLED building energy model, processed through the same RIME framework used for water and cooling CIDs.

Total energy demand is decomposed into climate and non-climate components.
The CID replacement preserves non-climate drivers (efficiency improvements, AC adoption, behavioral changes) while updating the climate response:

.. math::

   E(t, r, a) = \gamma(t, r, a) \times \text{EI}(r, a, \text{GSAT}(t)) \times F(t, r, a)

where:

- :math:`\gamma` is the correction coefficient (dimensionless), capturing non-climate drivers
- :math:`\text{EI}` is RIME energy intensity at year-specific GSAT (MJ/m²)
- :math:`F` is floor area from STURM projections (Mm²)
- :math:`t` is year, :math:`r` is R12 region, :math:`a` is building archetype

The coefficient :math:`\gamma` isolates non-climate drivers by comparing baseline demand to a reference climate state (:math:`\text{GWL} = 1.2°\text{C}`):

.. math::

   \gamma(t, r, a) = \frac{E_{\text{STURM}}(t, r, a)}{C(r, a, \text{GWL}=1.2) \times F(t, r, a)}

Time variation in :math:`\gamma` reflects efficiency improvements, AC adoption changes, and policy interventions—dynamics independent of climate change.

Residential buildings use full archetype resolution (``sfh_s1``, ``mfh_s2``, etc.) with direct EI lookup.
Commercial buildings use MFH-weighted average EI following STURM modeling conventions.

Only the climate-sensitive fraction of existing demand is replaced, preserving non-building sources (industrial cooling, district heating feedstocks).

The following MESSAGE parameters are modified:

.. list-table::
   :header-rows: 1
   :widths: 25 25 25 25

   * - Parameter
     - Commodity
     - Dimensions
     - Unit
   * - ``demand``
     - ``rc_spec`` (cooling)
     - node, year, time
     - GWa
   * - ``demand``
     - ``rc_therm`` (heating)
     - node, year, time
     - GWa

GMT Range and Clipping
======================

RIME emulators have empirical support for :math:`\text{GMT} \in [0.6, 7.4]°\text{C}`.
This is becasuse they are derived from ISIMIP3b datasets which cover ssp-rcp : 126,370,585 over 5 GCMs gfdl-esm4, ipsl-cm6a-lr, mpi-esm1-2-hr
,mri-esm2-0, ukesm1-0-ll.

Overshoot scenarios see temperatures fall in late century.
This means very lower emissions scenarios, exit the support (ex: sub 1000 Gton full century budget scenarios).
In general we choose the reference range 1100f - 2350f to cover 250 Gton intervals. For subsequent work VLLO style scenarios would be recommended.

Mitigation strategy:

- Annual emulators: clip GMT below :math:`0.6°\text{C}` to :math:`[0.6, 0.9]°\text{C}` with :math:`\text{Beta}(2,5)` noise
- Seasonal emulators: clip to :math:`[0.8, 1.2]°\text{C}` (higher threshold due to NaN coverage at low GWL)

Scenario Generation
===================

CID scenarios are generated from YAML configuration files via :program:`mix-models alps scen-gen`.

Example configuration:

.. code-block:: yaml

   starter:
     model: SSP_SSP2_v6.5_CID
     scenario: nexus_baseline_seasonal

   output:
     model: SSP_SSP2_v6.5_CID
     scenario_template: "nexus_baseline_{budget}_{temporal}"

   cid_type: nexus  # or 'cooling', 'buildings'

   rime:
     n_runs: 100
     variables:
       - qtot_mean
       - qr

   scenarios:
     - budget: null  # baseline emissions trajectory
       temporal: [annual, seasonal]
     - budget: 850f
       temporal: [annual, seasonal]

Budget levels are 600f, 850f, 1100f, 1350f, 1850f, 2100f, 2350f (GtCO2 carbon budgets).
The suffix indicates climate trajectory for CID computation, not a carbon constraint in MESSAGE.

Temporal resolution options:

- ``annual``: Single timeslice per year
- ``seasonal``: h1/h2 timeslices (water CIDs only; cooling and buildings are annual-only)

Special budget values:

- ``null`` or ``baseline``: CID from baseline emissions trajectory

CLI Reference
=============

.. code-block:: text

   mix-models alps [OPTIONS] COMMAND [ARGS]...

.. list-table::
   :header-rows: 1
   :widths: 20 50 30

   * - Command
     - Description
     - Key Options
   * - ``emissions-report``
     - Run emissions reporting & general legacy reporting workflow
     - ``--model``, ``--scenario``, ``--run-config``
   * - ``run-magicc``
     - Run MAGICC climate processing
     - ``--scenario``, ``--run-type``, ``--workers``
   * - ``scen-gen``
     - Generate CID scenarios from YAML
     - ``--config``, ``--budgets``, ``--temporal``
   * - ``report``
     - Run water/cooling reporting
     - ``--model``, ``--scenario``, ``--key``
   * - ``report-batch``
     - Batch reporting for multiple scenarios
     - ``--model``, ``--pattern``

Example workflow:

.. code-block:: bash

   # 1. Run emissions reporting
   mix-models alps emissions-report \
     --model SSP_SSP2_v6.5_CID \
     --scenario baseline

   # 2. Run MAGICC climate
   mix-models alps run-magicc \
     --scenario SSP_SSP2_v6.5 \
     --run-type medium

   # 3. Generate CID scenarios
   mix-models alps scen-gen \
     --config scenario_config.yaml \
     --budgets 850f,1100f \
     --temporal annual

   # 4. Report results (after solving)
   mix-models alps report \
     --model SSP_SSP2_v6.5_CID \
     --scenario nexus_baseline_850f_annual

Code Reference
==============

Pipeline
--------

.. autofunction:: message_ix_models.project.alps.scenario_generator.generate_scenario

.. autofunction:: message_ix_models.project.alps.scenario_generator.generate_all

.. autofunction:: message_ix_models.project.alps.rime.predict_rime

.. autofunction:: message_ix_models.project.alps.cid_utils.cached_rime_prediction

CID Replacement
---------------

.. autofunction:: message_ix_models.project.alps.replace_water_cids.prepare_water_cids

.. autofunction:: message_ix_models.project.alps.replace_water_cids.replace_water_availability

.. autofunction:: message_ix_models.project.alps.replace_cooling_cids.add_jones_relation_constraints

.. autofunction:: message_ix_models.project.alps.replace_cooling_cids.generate_cooling_cid_scenario

.. autofunction:: message_ix_models.project.alps.replace_building_cids.generate_building_cid_scenario

Reporting
---------

.. autofunction:: message_ix_models.project.alps.report.prepare_water_reporter

.. autofunction:: message_ix_models.project.alps.report.report_water_nexus

Risk Metrics
------------

.. autofunction:: message_ix_models.project.alps.cvar.compute_cvar

CLI
---

.. automodule:: message_ix_models.project.alps.cli
   :members:

Data Files
==========

RIME Datasets
-------------

Location: :file:`message_ix_models/data/alps/rime_datasets/`

Naming convention: ``rime_regionarray_{variable}_{hydro_model}_{temporal}_{window}.nc``

- ``variable``: ``qtot_mean``, ``qr``, ``temp_mean_anomaly``
- ``hydro_model``: ``CWatM``, ``H08``, ``WaterGAP2-2e``, ``MIROC-INTEG-LAND``
- ``temporal``: ``annual``, ``seasonal2step``
- ``window``: smoothing window (``window0`` = raw, ``window11`` = 11-year)

Regional datasets:

- :file:`r12_capacity_gwl_ensemble.nc`: Thermoelectric capacity factor (12 regions × 67 GWL bins)
- :file:`region_EI_cool_gwl_binned.nc`: Cooling energy intensity (12 regions × archetypes × GWL)
- :file:`region_EI_heat_gwl_binned.nc`: Heating energy intensity

The file :file:`joint_bifurcation_mapping_CWatM_2step.csv` specifies dry/wet month assignments per basin.

Building Parameters
-------------------

Location: :file:`message_ix_models/data/alps/`

- :file:`sturm_params/`: STURM baseline energy and parameters by year
- :file:`correction_coefficients/`: γ coefficients for S1/S2/S3 scenarios, residential/commercial
- :file:`sturm_floor_area_R12_{resid,comm}.csv`: Floor area projections
- :file:`rc_sector_fractions.csv`: Residential/commercial demand decomposition

Reporting Config
----------------

Location: :file:`message_ix_models/data/alps/report/water_cooling.yaml`

Genno configuration for water/cooling aggregations and technology groupings.

Project Information
===================

IIASA roles (ALPS 16 PART B):

- Project lead: :gh-user:`byersiiasa`
- Lead modelers: :gh-user:`wegatriespython`, :gh-user:`adrivinca`

IIASA roles (ALPS 15):

- Project lead: :gh-user:`byersiiasa`
- Lead modelers: :gh-user:`giacfalk`, :gh-user:`amastrucci`; formerly :gh-user:`measrainsey`

.. seealso::

   :doc:`m-data:project/alps` in the (non-public) :mod:`message_data` documentation.
