.. _water-climate-impacts:

Climate Change Impacts
======================

A key feature of the MESSAGEix-Nexus module is that water availability is not a fixed boundary condition: it is read from climate-driven hydrological projections and therefore changes with the forcing scenario (Awais et al., 2024 :cite:`awais_2024_nexus`). This enables analysis of adaptation strategies, of compound risks at the water-energy nexus, and of the interaction between climate impacts and mitigation policy.

Overview
--------

Climate change reaches the water-energy-land nexus through several pathways:

**Direct impacts on water**:

* Changes in precipitation patterns and amounts
* Shifts in snowmelt timing and magnitude
* Altered groundwater recharge rates
* Increased evapotranspiration
* More frequent and severe droughts
* Changes in seasonal water availability

**Nexus interactions**:

* Water scarcity constrains thermal power plant operation and siting
* Competing demands for limited water resources intensify
* Adaptation measures in one sector affect the other

Of these, the pathway currently represented in the model is basin water availability. Changes in availability alter the cost and feasibility of water supply in each basin, which propagates into cooling technology choice and generation mix. The direct effect of ambient temperature on plant and cooling system performance is not currently represented — see :ref:`climate-energy-impacts` below.

Climate Forcing and Scenarios
------------------------------

Forcing Scenarios
^^^^^^^^^^^^^^^^^

Water availability is supplied as a fixed set of CWatM runs over the SSP-RCP scenario combinations. Three forcing scenarios are shipped, drawn from the ISIMIP3b protocol and reported in the model under the legacy RCP labels (van Vuuren et al., 2011 :cite:`vanvuuren_2011_rcp`):

.. list-table:: Available climate forcing scenarios
   :widths: 25 25 50
   :header-rows: 1

   * - Model setting
     - ISIMIP3b scenario
     - Approximate forcing
   * - ``2p6``
     - ssp126
     - Strong mitigation
   * - ``7p0``
     - ssp370
     - Weak mitigation, high emissions
   * - ``8p5``
     - ssp585
     - Very high emissions

The default setting is ``2p6``. There is no unforced or "no climate" option: every build reads a climate-driven availability series.

Because these are pre-computed hydrological runs, a scenario whose emissions pathway does not correspond to one of the three shipped forcing levels has to be approximated by the nearest available one. An **experimental emulation of arbitrary emission pathways** exists for cases where that approximation is too coarse — for example when assessing a mitigation pathway that sits between ``2p6`` and ``7p0``. It is not part of the shipped data and is available on request.

Shared Socioeconomic Pathways
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The module is applied within the SSP framework (O'Neill et al., 2014 :cite:`oneill_new_2014`; Riahi et al., 2017 :cite:`riahi_shared_2017`). The SSP of a run determines the sectoral water demand trajectories, the drinking-water access rate projections and the desalination capacity projection, while the forcing scenario determines water availability. The two are set independently, so a scenario pairs a socioeconomic pathway with a level of climate forcing.

Climate Model Ensemble
^^^^^^^^^^^^^^^^^^^^^^

To account for climate model uncertainty, the hydrological simulations are driven by a five-member GCM ensemble from ISIMIP3b, bias-adjusted against the W5E5 observational dataset:

* GFDL-ESM4
* IPSL-CM6A-LR
* MPI-ESM1-2-HR
* MRI-ESM2-0
* UKESM1-0-LL

The ensemble is collapsed to a cross-model mean per basin and month before the temporal statistics described below are taken. The ensemble spread is therefore not propagated into the model; it is averaged out.

Hydrological Impacts
--------------------

Changes in water availability are the climate impact the module represents directly.

Hydrological Model Framework
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Water availability is simulated with **CWatM**, the Community Water Model (Burek et al., 2020 :cite:`burek_2020_cwatm`), a spatially distributed global hydrological model that represents water availability, demand and allocation, includes reservoirs and water management, and can simulate environmental flows. CWatM is run on a 0.5 degree global grid, driven by the ISIMIP3b ensemble described above, and provides total runoff, groundwater recharge and environmental flow requirements.

Spatial Aggregation
^^^^^^^^^^^^^^^^^^^

Gridded outputs are aggregated onto the MESSAGE basin delineation:

1. Gridded CWatM outputs on the 0.5 degree global grid
2. Basin delineation derived from HydroSHEDS
3. Aggregation to MESSAGE basin capacity units (BCUs)
4. Mapping of basins to MESSAGE regions (R12)

Three basins are entirely missing from the hydrological source and are excluded from the basin set.

Temporal Aggregation and Reliability
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Monthly basin series are reduced to the five-yearly values MESSAGE uses, from 2015 to 2100, in two forms: an annual value per five-year period, and a seasonal cycle giving a value per calendar month within each period. Sub-annual builds use the latter.

The annual value is not a mean but a **quantile of the monthly distribution**, selected by the run's reliability setting. The setting names refer to the level of water stress, not the level of availability, so the quantiles run in the opposite direction to what the names suggest:

.. list-table:: Reliability settings
   :widths: 25 25 50
   :header-rows: 1

   * - Setting
     - Quantile
     - Interpretation
   * - ``low``
     - 50th percentile
     - Median availability, lowest stress
   * - ``med``
     - 30th percentile
     - Reduced availability
   * - ``high``
     - 10th percentile
     - Tail of low availability, highest stress

This is the mechanism through which drought conditions enter the model. Running at ``high`` reliability tests the system against sustained low flows rather than against average conditions, and climate change increases the frequency and severity of such conditions (Satoh et al., 2022 :cite:`satoh_2022_drought`).

Key Hydrological Impact Patterns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Climate change impacts on water availability exhibit strong regional patterns (Awais et al., 2024 :cite:`awais_2024_nexus`):

**Wetting regions** (increased water availability):

* High northern latitudes (more precipitation, earlier snowmelt)
* Parts of East Africa (intensified monsoons)
* Some tropical regions (increased convective precipitation)

**Drying regions** (decreased water availability):

* Mediterranean basin (reduced precipitation, increased evaporation)
* Middle East and North Africa (lower precipitation)
* Southern Africa (decreased precipitation)
* Parts of South America (Amazon, Northeast Brazil)
* Southwestern USA (reduced snowpack, increased evaporation)

**Seasonal shifts** (changed timing of availability):

* Snow-dominated basins (earlier snowmelt peak, lower summer flows)
* Monsoon regions (shifts in monsoon timing and intensity)
* Mediterranean climate regions (drier summers, wetter winters)

**Increased variability**:

* More frequent and intense droughts
* Increased interannual variability
* Higher flood risks (not represented in MESSAGE)

.. _climate-energy-impacts:

Energy System Impacts
---------------------

.. note:: **Placeholder — to be completed.**

   Earlier versions of the module applied a climate- and region-dependent
   impact factor to the capacity factor of freshwater-cooled power plant
   technologies, so that warming degraded cooling performance directly. That
   representation has been removed from the build pending its replacement, and
   the cooling technology parameterisation is currently independent of the
   climate forcing scenario.

   This section should describe the replacement representation once it lands.
   The effects it is expected to cover include:

   * Reduced thermal power plant efficiency at higher ambient temperatures
   * Higher cooling water temperatures constraining once-through cooling
   * Changes in hydropower generation potential
   * Shifts in electricity demand between heating and cooling

   In the meantime, climate change still reaches the energy system in the
   model, but only indirectly: a drier basin raises the cost and tightens the
   availability of the water that wet cooling requires, which shifts cooling
   technology choice and the generation mix.

Compound Events
^^^^^^^^^^^^^^^

Heat and drought frequently coincide, and their joint effect on the water-energy system is larger than either alone: electricity demand rises, water availability falls, and the cooling options that use least water are the ones whose performance degrades most in heat. The module captures the water availability leg of this directly. The temperature legs depend on the representation described above, so compound events are currently only partially represented.

Adaptation Strategies
---------------------

The model can endogenously select a range of responses to climate-driven water stress.

Water Supply Adaptation
^^^^^^^^^^^^^^^^^^^^^^^

* Investment in desalination where basins have access to saline water, subject to the capacity and growth bounds described in :doc:`supply`
* Increased wastewater treatment and recycling
* Shifts between surface water and groundwater within the sustainability share constraint
* Use of fossil groundwater as a residual backstop at a cost premium

Energy System Adaptation
^^^^^^^^^^^^^^^^^^^^^^^^

* Shifts from once-through to recirculating or dry cooling
* Expansion of generation technologies that need no cooling water, principally wind and solar PV
* Changes in the siting of new thermal capacity between basins

Demand-Side Adaptation
^^^^^^^^^^^^^^^^^^^^^^

* Water use efficiency improvements in municipal and industrial use
* Irrigation efficiency improvements
* Reallocation of water between sectors as relative scarcity changes

Adaptation Costs and Limits
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Adaptation is not costless: desalination carries both capital costs and a substantial electricity requirement, dry cooling carries a capital premium and an efficiency penalty, and efficiency improvements require upfront investment. Where climate forcing is strong, these costs concentrate in the regions that are already water-stressed.

There are also limits beyond which adaptation cannot go within the model:

* Desalination capacity cannot expand faster than its growth bound, and is unavailable in basins without a projection
* Surface water and renewable groundwater activity cannot grow faster than 2% per year
* Renewable groundwater use is bounded below by the sustainability share constraint
* Social and institutional barriers to demand reduction are not represented at all

Uncertainty and Robustness
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The main uncertainties in this representation are:

* **Climate model spread**: regional precipitation change is the least agreed-upon output of climate models, and the module averages across the five-member ensemble rather than propagating the spread
* **Hydrological model structure**: a single hydrological model is used, so structural uncertainty in the hydrology is not sampled
* **Socioeconomic assumptions**: the SSP determines both the demand trajectory and the adaptive capacity available to meet it
* **Technology development**: costs and performance of desalination, dry cooling and efficiency measures

Strategies that perform well across scenarios — renewable energy expansion, water use efficiency, and a diversified supply portfolio — are correspondingly more robust than those that depend on a particular hydrological outcome.

Future Development
------------------

Planned enhancements to the climate impact representation:

**Energy system impacts**: the replacement for the removed power plant impact representation, described above, is the immediate priority.

**Enhanced hydrology**:

* More detailed reservoir and water management representation
* Groundwater-surface water interactions
* Water quality and temperature tracking

**Extremes and risks**:

* Propagation of the climate model ensemble spread rather than the ensemble mean
* Explicit flood representation
* Cascading infrastructure failures

.. footbibliography::
