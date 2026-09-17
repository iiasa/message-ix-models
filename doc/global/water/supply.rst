.. _water-supply:

Water Supply
============

Water supply in MESSAGEix-Nexus is represented through multiple technology options that extract, treat, and distribute freshwater from surface and groundwater sources, as well as non-conventional sources such as desalination and treated wastewater reuse (Awais et al., 2024 :cite:`awais_2024_nexus`). Each basin has specific renewable water availability derived from hydrological model outputs, which constrains total water extraction.

Surface Water
-------------

Surface water resources include runoff from precipitation, snowmelt, and glacier melt aggregated at the river basin scale. Surface water availability is represented as a time-varying resource potential for each basin.

Hydrological Data Sources
^^^^^^^^^^^^^^^^^^^^^^^^^^

Basin-scale surface water availability is derived from the **CWatM** Community Water Model (Burek et al., 2020 :cite:`burek_2020_cwatm`), a spatially distributed global hydrological model that simulates runoff, groundwater recharge and environmental flows.

CWatM is driven by the **ISIMIP3b** climate forcing ensemble:

* **Five global climate models**: GFDL-ESM4, IPSL-CM6A-LR, MPI-ESM1-2-HR, MRI-ESM2-0 and UKESM1-0-LL, bias-adjusted against W5E5
* **Three forcing scenarios**: ssp126, ssp370 and ssp585, reported in the model under the legacy RCP labels 2p6, 7p0 and 8p5
* **Spatial resolution**: 0.5 degree global grid
* **Projection period**: five-yearly values from 2015 to 2100

Gridded monthly outputs are aggregated onto the MESSAGE basin delineation, the ensemble is collapsed to a cross-model mean per basin and month, and the resulting series is reduced to the five-yearly values used by MESSAGE (Awais et al., 2024 :cite:`awais_2024_nexus`). Three basins are entirely missing from the hydrological source and are excluded from the basin set.

Temporal Variability
^^^^^^^^^^^^^^^^^^^^

Surface water availability exhibits strong seasonal and interannual variability:

* **Seasonal patterns**: Monsoon regions show pronounced wet/dry seasons; snow-dominated basins have spring snowmelt peaks
* **Interannual variability**: Represented through statistical analysis of multi-year hydrological simulations
* **Climate trends**: Long-term changes in mean availability and variability under different climate scenarios
* **Extreme events**: Droughts represented as low quantiles of the flow distribution

Availability is supplied at three **reliability** levels, selected per run. The labels refer to the level of water stress, not to the level of availability, so the quantiles are inverted relative to what the names suggest:

.. list-table:: Reliability settings and the underlying quantile
   :widths: 25 25 50
   :header-rows: 1

   * - Setting
     - Quantile of the monthly series
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

For sub-annual MESSAGE implementations, seasonal water availability is explicitly represented. For annual implementations, the five-yearly value at the selected reliability level is used.

Environmental Flow Requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Not all renewable surface water can be extracted for human use. Environmental flow requirements (EFRs) are subtracted from gross water availability to determine the extractable potential:

:math:`SW_{extract,b,t} \leq SW_{available,b,t} - EFR_{b,t}`

where :math:`SW_{extract,b,t}` is extractable surface water in basin :math:`b` and time period :math:`t`, :math:`SW_{available,b,t}` is total renewable surface water, and :math:`EFR_{b,t}` is the environmental flow requirement.

Environmental flows are calculated using the Variable Monthly Flow (VMF) method (Pastor et al., 2014 :cite:`pastor_2014_efr`), which sets minimum flows as a percentage of mean monthly natural flow, with higher percentages for low-flow months to protect aquatic ecosystems. Typical EFR values range from 20-40% of mean annual flow depending on the basin and flow regime.

Surface Water Extraction Technologies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Surface water extraction is represented through technology archetypes with associated costs and infrastructure requirements:

* **River/lake extraction**: Direct abstraction with intake structures, screening, and pumping
* **Large-scale reservoir storage**: Represented through hydropower technologies in MESSAGE

Extraction is parameterised with investment and fixed costs, and with an electricity input representing the energy needed to abstract and convey water. Growth in surface water extraction activity is limited to 2% per year, so the supply mix cannot restructure instantaneously.

Groundwater
-----------

Groundwater provides a critical buffer against surface water variability and is explicitly represented in MESSAGEix-Nexus with depth-dependent extraction costs and sustainability constraints.

Groundwater Resources
^^^^^^^^^^^^^^^^^^^^^^

Groundwater resources are characterized by:

* **Renewable groundwater**: Annual recharge from precipitation infiltration and river seepage
* **Non-renewable (fossil) groundwater**: Deep aquifers with negligible recharge on human timescales
* **Groundwater storage**: Cumulative volume in aquifers (not fully represented in current implementation)

Renewable groundwater recharge is derived from the same CWatM simulations as surface water, which represent infiltration, percolation and recharge processes, and is supplied per basin on the same five-yearly grid.

Groundwater Extraction
^^^^^^^^^^^^^^^^^^^^^^

Groundwater extraction costs depend on:

1. **Aquifer depth**: Pumping costs increase with depth (energy requirements)
2. **Extraction rate**: Higher rates require more/deeper wells
3. **Water quality**: Treatment requirements for brackish or contaminated groundwater

Groundwater extraction is parameterised per basin from a harmonised table of pumping energy intensities, which reflect basin-specific water table depth, plus a uniform adder representing the energy needed to lift and convey the extracted water. This electricity input creates the water-energy feedback loop: deeper aquifers draw more electricity per unit of water, which in turn adds to the load the energy system must serve.

Growth in renewable groundwater extraction activity is limited to 2% per year, matching the constraint on surface water.

Groundwater Sustainability Constraints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sustainable use is enforced through a **share constraint** rather than a cumulative volume balance. In each basin and period, renewable groundwater must supply at least its recharge-implied share of total renewable availability:

:math:`share_{GW,b,t} \geq \dfrac{GW_{recharge,b,t}}{SW_{available,b,t} + GW_{recharge,b,t}} \cdot 0.95`

The 0.95 factor leaves headroom against numerical error in the optimisation. The same expression is used both for this in-horizon constraint and for the historical calibration described below, so the two cannot drift apart.

Fossil (non-renewable) groundwater is represented as a separate extraction technology acting as a **residual backstop**: it is available when renewable sources cannot meet demand, and is priced at a 20% premium over renewable groundwater on investment cost and pumping electricity, with the same 20-year technical lifetime. It is therefore unattractive relative to renewable sources, but not prohibited outright. Basins that draw on this backstop correspond to regions of known aquifer depletion, such as the Indus-Ganges basin, the North China Plain, the Arabian Peninsula and the High Plains Aquifer.

Desalination
------------

Desalination converts saline water into freshwater, providing a water source that is independent of basin hydrology. It is explicitly represented in MESSAGEix-Nexus (Awais et al., 2024 :cite:`awais_2024_nexus`) and is a key option for water-scarce coastal basins.

Desalination Technologies
^^^^^^^^^^^^^^^^^^^^^^^^^^

Two technology categories are represented:

**Membrane desalination** (reverse osmosis): membrane-based separation driven by electricity. It is modular, scalable, and the technology of choice for most new capacity.

**Distillation** (thermal processes such as MSF and MED): evaporation-based separation driven by heat, which can draw on waste heat from co-located thermal power generation. Historically dominant, it remains significant in the Middle East.

Both draw on a shared saline water extraction technology, so their combined activity is limited by the saline extraction capacity available in the basin.

Capacity and Projections
^^^^^^^^^^^^^^^^^^^^^^^^^

Historical desalination capacity and its future projection are supplied as exogenous basin-level data, downscaled from country-level sources:

* **Projections are keyed on the SSP**, not on the climate forcing scenario. Source projections exist for SSP1, SSP3 and SSP5 only; SSP2 uses the SSP1 projection and SSP4 uses the SSP3 projection.
* **Basins with no projection have zero capacity.** An absent basin-year entry is treated as a hard zero on saline water extraction rather than leaving extraction unconstrained.
* **Historical capacity sets an activity floor** in the early model periods. Where the membrane and distillation floors together would exceed the shared saline extraction cap, both are scaled down proportionally so the two are consistent.
* **New capacity growth is limited to 10% per year**, which smooths the vintage-replacement sawtooth that otherwise appears in basin-level desalination capacity.

Beyond these bounds, capacity expansion is endogenous and responds to water scarcity, the cost and availability of alternative sources, energy prices, and climate impacts on conventional supply.

Energy Requirements
^^^^^^^^^^^^^^^^^^^

Desalination is energy-intensive, and this is where it enters the nexus: membrane desalination draws electricity, distillation draws heat, and both therefore compete for energy that the rest of the system also needs. Deploying desalination at scale in a water-scarce basin raises that basin's energy demand, which in turn has its own water requirements for cooling.

Wastewater Treatment and Reuse
-------------------------------

Treated wastewater provides an additional water source, particularly for non-potable uses such as industrial cooling and irrigation.

Return flows from municipal and industrial use are tracked explicitly. The fraction of those flows that is collected and treated, and the fraction that is subsequently recycled back into supply, are set by exogenous **treatment and recycling rates** specified per region and scenario:

* Urban treatment rate
* Rural treatment rate
* Urban recycling rate

These rates are not differentiated across SSPs — every SSP reads the SSP2 values. Treatment requires energy and capital, so higher treatment and recycling ambition raises both the cost of the water system and its electricity demand.

The economic attractiveness of reuse depends on the cost of alternative water sources in the same basin, the stringency of the assumed treatment requirement, and the proximity of return flows to the demands that could use them.

Water Supply Portfolio
-----------------------

The model endogenously selects the portfolio of water supply technologies in each basin, subject to:

* Resource availability at the selected reliability level
* Technology costs and energy requirements
* The groundwater sustainability share constraint
* Desalination capacity bounds and growth limits
* A 2% per year limit on growth in surface water and renewable groundwater activity

Historical Calibration
^^^^^^^^^^^^^^^^^^^^^^

The starting point of the portfolio is not left to the optimisation. Historical extraction activity is seeded by a **merit-order dispatch**: historical sectoral and irrigation demand in each basin is met from the available sources — surface water, renewable groundwater and fossil groundwater — in order of operating cost, subject to historical basin capacity and to the same groundwater share floor that applies in the model horizon. This anchors the base-year supply mix to something defensible and prevents the first model period from restructuring the water system implausibly fast.

Because fossil groundwater is now priced as a residual backstop rather than penalised outright, this calibration attributes more use to fossil groundwater than earlier versions of the model did — consistent with observed aquifer depletion in the basins concerned.

.. footbibliography::

