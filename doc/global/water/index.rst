.. _water_nexus:

Water-Energy-Land Nexus (MESSAGEix-Nexus)
******************************************

The MESSAGEix-GLOBIOM nexus module (MESSAGEix-Nexus) integrates water sector representation and climate impacts into the |MESSAGEix|-GLOBIOM integrated assessment modeling framework (Awais et al., 2024 :cite:`awais_2024_nexus`). This comprehensive nexus implementation enables consistent analysis of interdependencies between water, energy, and land systems under different climate and socioeconomic scenarios.

MESSAGEix-Nexus builds upon earlier water-energy linkages (Parkinson et al., 2019 :cite:`parkinson_2019`; Vinca et al., 2020 :cite:`vinca_2020_nest`) by adding:

* Basin-scale water resource representation (surface water and groundwater)
* Water demands from multiple sectors (energy, municipal, industrial, irrigation)
* Water supply technologies (surface water extraction, groundwater extraction, desalination, wastewater treatment)
* Power plant cooling technology options with explicit water-energy tradeoffs
* Climate change impacts on water availability and energy systems
* Sustainable Development Goal (SDG) constraints for water access

Overview
========

The nexus module represents water resources and demands at the spatial scale of ~200 river basins globally, while maintaining consistency with the 12-region spatial resolution of MESSAGEix-GLOBIOM (R12). Water is explicitly tracked through the energy system via cooling technologies for thermal power plants, while also accounting for sectoral water demands that compete with energy sector water use.

Basin-scale Spatial Resolution
-------------------------------

Water resources are represented at the basin scale using a global delineation of river basins derived from HydroSHEDS (Lehner et al., 2008 :cite:`lehner_2008_hydrosheds`). Basins are mapped to MESSAGE regions through spatial intersection, enabling consistent aggregation of basin-level constraints to regional energy system decisions. This multi-scale approach captures:

* Spatial heterogeneity in water availability within MESSAGE regions
* Local water scarcity constraints that affect technology choices
* Inter-basin water transfers where infrastructure exists
* Climate impacts on basin-specific hydrology

:numref:`fig-global-basin-map` shows the global distribution of approximately 200 basins mapped to the 12 MESSAGE regions (R12), providing the spatial foundation for the nexus module.

.. _fig-global-basin-map:
.. figure:: /_static/global_r12_basin_map.png
   :width: 800px
   :align: center

   Global basin delineation mapped to MESSAGE R12 regions. Basins are derived from HydroSHEDS and aggregated to provide spatially explicit water resource representation within the MESSAGEix-GLOBIOM framework (Awais et al., 2024 :cite:`awais_2024_nexus`).

Temporal Resolution
-------------------

The nexus module can operate at annual or sub-annual (seasonal/monthly) temporal resolution. For climate impact studies, sub-annual resolution is critical to capture:

* Seasonal variations in water availability (monsoons, snowmelt, dry seasons)
* Mismatches between seasonal water supply and demand
* Hydropower generation patterns and reservoir management
* Irrigation water requirements aligned with crop calendars

When sub-annual time steps are defined in the MESSAGE model, the water module automatically generates water balance constraints at the corresponding temporal resolution (Awais et al., 2024 :cite:`awais_2024_nexus`).

Model Structure
===============

The water nexus implementation follows a resource-technology-demand structure analogous to the energy system representation in MESSAGEix. The conceptual framework integrates water resources, supply technologies, sectoral demands, and the linkages between water, energy, and land systems as illustrated in :numref:`fig-nexus-structure`.

.. _fig-nexus-structure:
.. figure:: /_static/message_nexus_structure.png
   :width: 800px
   :align: center

   Conceptual structure of the MESSAGEix-GLOBIOM nexus module showing the integration of water resources, supply technologies, sectoral demands, and linkages with energy and land systems (Awais et al., 2024 :cite:`awais_2024_nexus`).

The nexus module comprises three main components:

**Resources**: Renewable surface water and groundwater availability in each basin and time period, derived from hydrological models. See :doc:`supply` for detailed description of water resources and supply technologies.

**Technologies**: Water extraction, treatment, conveyance, and end-use technologies including:

* Surface water extraction and distribution
* Groundwater extraction (with depth-dependent costs)
* Desalination (thermal and reverse osmosis)
* Wastewater treatment and reuse
* Power plant cooling technologies (once-through, recirculating, dry cooling)
* Irrigation technologies

**Demands**: Sectoral water requirements including:

* Energy sector (power plant cooling, fuel extraction and processing)
* Municipal and domestic water use
* Industrial manufacturing water use  
* Agricultural irrigation (linked to GLOBIOM)

See :doc:`demand` for comprehensive coverage of all sectoral water demands and allocation mechanisms.

Water Reference Energy System
------------------------------

The water flows through the model are represented using a Reference Energy System (RES) structure, extending the MESSAGEix energy RES to include water commodities and technologies. :numref:`fig-water-res` shows the simplified water reference energy system structure that connects water resources to sectoral demands through various supply technologies.

.. _fig-water-res:
.. figure:: /_static/water_reference_system.png
   :width: 800px
   :align: center

   Simplified water Reference Energy System (RES) showing the flow of water from resources (surface water, groundwater, desalination) through treatment and distribution to sectoral demands (municipal, industrial, agricultural, energy). The RES structure enables explicit tracking of water quantities, qualities, and associated costs and energy requirements (Awais et al., 2024 :cite:`awais_2024_nexus`).

Water balance equations ensure that total water extraction does not exceed renewable availability plus sustainable groundwater use, while meeting all sectoral demands and environmental flow requirements. The optimization simultaneously determines the least-cost portfolio of water supply technologies and the allocation of water across competing sectoral demands.

Energy-Water-Land Linkages
==========================

MESSAGEix-Nexus captures multiple nexus interactions:

**Energy → Water**: 

* Cooling water requirements for thermal power plants (see :doc:`cooling` for detailed cooling technology representation)
* Water consumption in fuel extraction (coal mining, unconventional oil and gas)
* Hydropower production from surface water resources
* Energy requirements for water supply (pumping, treatment, desalination)

**Water → Energy**:

* Water availability constraints on thermal power plant siting and operation
* Cooling technology choices driven by water scarcity
* Hydropower generation governed by river flows and reservoir storage
* Groundwater pumping costs dependent on aquifer depth

**Land → Water** (via GLOBIOM linkage):

* Irrigation water demands for crop production
* Land use change impacts on runoff and water availability
* Water allocation between agriculture and other sectors

**Climate → Water-Energy-Land**:

* Temperature and precipitation changes affecting water availability
* Extreme events (droughts, floods) impacting all sectors
* Climate-driven changes in cooling water requirements and efficiency
* Shifts in crop water demands and irrigation needs

Climate Change Impacts
======================

The nexus module incorporates climate change impacts on both water availability and energy systems (Awais et al., 2024 :cite:`awais_2024_nexus`):

**Hydrological Impacts**: Basin-specific changes in renewable water availability derived from global hydrological models (PCR-GLOBWB, CWatM) forced by climate model outputs. Impacts include:

* Changes in mean annual runoff and groundwater recharge
* Shifts in seasonal water availability patterns
* Increased frequency and severity of droughts
* Modified snowmelt timing in snow-dominated basins

**Energy System Impacts**:

* Reduced thermal power plant efficiency due to higher ambient temperatures
* Increased cooling water requirements from higher water temperatures
* Changes in hydropower generation potential
* Modified electricity demand patterns (cooling vs. heating)

**Adaptation Measures**: The model can endogenously select adaptation measures such as:

* Shifts to less water-intensive cooling technologies
* Investment in desalination and water reuse
* Inter-basin water transfers
* Changes in electricity generation technology mix

For comprehensive discussion of climate change impacts on the water-energy nexus, see :doc:`climate_impacts`.

Sustainable Development Goals
==============================

The nexus module includes optional constraints to represent progress toward water-related Sustainable Development Goals (Awais et al., 2024 :cite:`awais_2024_nexus`):

**SDG 6**: Clean water and sanitation

* Targets for urban and rural water access rates
* Wastewater treatment coverage requirements
* Water use efficiency improvements

**SDG 7**: Affordable and clean energy

* Energy access targets requiring water for cooling and hydropower
* Trade-offs between water and energy access in water-scarce regions

Implementation constraints enforce minimum investment in water supply infrastructure to achieve specified access targets in each region and time period, creating additional water demand and infrastructure requirements that compete with energy sector water use.

Detailed Documentation
=======================

For detailed technical documentation of the MESSAGEix-Nexus module components, please refer to the following sections:

* :doc:`supply` - Water resources and supply technologies (surface water, groundwater, desalination, wastewater reuse)
* :doc:`demand` - Sectoral water demands (energy, municipal, industrial, agricultural) and allocation
* :doc:`cooling` - Power plant cooling technologies and water-energy tradeoffs
* :doc:`climate_impacts` - Climate change impacts on water availability and energy systems

.. toctree::
   :maxdepth: 2
   :hidden:

   supply
   demand
   cooling
   climate_impacts

Reference
=========

The MESSAGEix-Nexus module is described in detail in:

Awais, M., Vinca, A., Byers, E., Frank, S., Fricko, O., Boere, E., Burek, P., Poblete Cazenave, M., Kishimoto, P.N., Mastrucci, A., Satoh, Y., Palazzo, A., McPherson, M., Riahi, K., and Krey, V. (2024). MESSAGEix-GLOBIOM nexus module: integrating water sector and climate impacts. *Geoscientific Model Development*, 17, 2447-2469. https://doi.org/10.5194/gmd-17-2447-2024

The NEST (Nexus Solutions Tool) framework that preceded this implementation is described in:

Vinca, A., Parkinson, S., Byers, E., Burek, P., Khan, Z., Krey, V., Diuana, F.A., Wang, Y., Ilyas, A., Köberle, A.C., Staffell, I., Pfenninger, S., Muhammad, A., Rowe, A., Schaeffer, R., Rao, N.D., Wada, Y., Djilali, N., and Riahi, K. (2020). The NExus Solutions Tool (NEST) v1.0: an open platform for optimizing multi-scale energy–water–land system transformations. *Geoscientific Model Development*, 13, 1095-1121. https://doi.org/10.5194/gmd-13-1095-2020

Power plant cooling implementation is described in:

Parkinson, S., Byers, E., Gidden, M., Krey, V., Burek, P., Vollmer, D.,Jalava, M., Palazzo, A., Graham, N., Fricko, O., Tracking the water-energy-land-food nexus: integrated assessment of sustainable development goal interactions. Submitted to *Nature Sustainability*. 2019.

.. footbibliography::
