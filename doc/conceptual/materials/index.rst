MESSAGEix-Materials
*******************

MESSAGEix-Materials is a drop-in alternative model for the original MESSAGEix-GLOBIOM-GAINS industry representation.
It augments the MESSAGEix-GLOBIOM-GAINS model structure in several aspects through replacing and complementing it with industry-specific elements.
In doing so, the sectoral representation of energy demand commonly used in energy system models is disaggregated into relevant industrial sectors.
A specific model structure is introduced for each of the four energy- and emission-intensive industries (steel, aluminium, cement, and chemicals), while the remaining industries stay in the general representation.
Although all four industrial sectors have their own model structure, they are all characterized by a common methodology.
Instead of representing industrial activity primarily through energy demand, the materials formulation explicitly represents demand for physical materials in each sector, which then determines the associated energy and material flows.
Furthermore, the technology portfolio is extended with technologies that represent the key production processes of each industry.
The output and input of energy, intermediate products, and final products are thus explicitly represented as material and energy flows.
This goes beyond the conventional energy-demand formulation: energy demand associated with the modeled material production is estimated endogenously through the material representation.

This change enables a more disaggregated representation of industry:
The tailored sector representations enable the targeted analysis of individual activities.
This allows sector-specific efficiency and emission reduction potentials to be modeled accurately, for example the introduction of hydrogen in steel production or the use of alternative fuels in the cement industry.
This allows for differentiated costs and the consideration of physical and technological constraints.

.. note::

   The version described here is MESSAGEix-Materials v1.2.0 (Ünlü et al., 2024 :cite:`unlu_2024_materials`).
   The paper and its supplementary information are the primary sources for the model formulation and data assumptions:

   * :download:`Ünlü et al. (2024), main paper
     <../../../gmd-17-8321-2024.pdf>`;
   * :download:`Ünlü et al. (2024), supplementary information
     <../../../gmd-17-8321-2024-supplement.pdf>`.

   This page describes the model conceptually, following the structure used for the :doc:`global MESSAGEix-GLOBIOM-GAINS model </global/index>`.
   The :doc:`technical model page </material/index>` documents the Python implementation, input files, and commands used to build and solve a scenario.

.. contents::
   :local:

Overview
========

The global MESSAGEix-GLOBIOM-GAINS model represents energy supply, conversion, transport, and end-use services.
The conventional global model does not explicitly represent material flows and stocks besides fossil fuels.
MESSAGEix-Materials adds explicit stock-flow dynamics for selected material-intensive activities while retaining the MESSAGE linear optimization framework and the regional structure of the underlying model.
Industrial production not covered by the explicit material sectors remains in the generalized MESSAGE industry representation, preserving coverage of total industrial energy demand of the system.

The model explicitly represents four groups of material-intensive activities:

* iron and steel;
* cement and other non-metallic minerals;
* aluminum; and
* primary chemicals, including ammonia, methanol, and high-value chemicals (HVCs).

The model also represents material requirements associated with the construction and retirement of power-generation technologies.
Industrial activities not covered by the explicit material sectors remain in the generalized MESSAGE industry representation.
This preserves coverage of total industrial energy demand.

.. _materials-spatial:

Spatial resolution
==================

MESSAGEix-Materials uses the spatial resolution of the underlying MESSAGEix-GLOBIOM-GAINS model.
The global model is commonly operated with 13 regions, including 12 geographical regions and a global trade region (see :ref:`spatial`).
Production, consumption and trade are represented at the model-region level.

The explicit material sectors do not resolve individual plants or intra-regional supply chains.

.. _materials-temporal:

Time horizon and dynamics
=========================

The material extension uses the model years and time periods of the MESSAGEix-GLOBIOM-GAINS scenario on which it is built.
Technology vintages, capacity lifetimes, investment, activity, and retirement are represented using MESSAGE's standard time structure.
For the 2020 base year, the model is calibrated against available energy and industry statistics, including energy use, material production, consumption and trade.
Ore extraction is currently not calibrated as a represented process: the associated extraction energy use, physical losses, and waste are outside the current model boundary.

To account for stock dependent flow dynamics, the model can make use of the recent commodity balance extension with capacity-based commodity flows.
This allows to better represent the dynamics of material stocks and flows, including the accumulation of material in use and the availability of secondary materials from retired stocks.
Material inflows add to stocks in use, while retirements generate outflows that may become available for collection, recycling, or disposal.
The resulting availability of secondary material depends on the stock-flow assumptions and on the technologies included in the scenario.

Socio-economic drivers
======================

Population and GDP projections are inherited from the underlying MESSAGEix-GLOBIOM-GAINS scenario.
Together with the associated SSP narrative, these projections determine the development of material demand and the services that require material stocks.
Demand assumptions are combined with historical calibration data and technology-specific parameters.

System definition
=================

Traditional energy-system models focus on energy commodities as inputs to socio-economic processes and track their implications for greenhouse-gas emissions.
MESSAGEix-Materials extends this perspective with an economy-wide material flow analysis (MFA) of material cycles and material stocks.
The conceptual system definition draws on the industrial-ecology concept of social metabolism, in which society requires material and energy inputs to build, maintain, and reproduce its biophysical stocks, while producing waste and emissions.

The fundamental accounting principle is conservation of mass.
All material inputs to a system over a given period must equal all outputs over the same period, plus or minus changes in material stocks.
This principle requires a clearly defined system boundary, including the processes, stocks, and flows that are represented.
In MESSAGEix-Materials, the boundary is defined across the physical Earth system and the socio-economic system represented in material flow accounts.

.. todo:: Insert Figure 2 from Ünlü et al. (2024), showing the generic
   representation of material flows and stocks in MESSAGEix-Materials.

The generic system framework consists of a sequence of industrial and end-use processes:

* **raw-material extraction (P1)** brings non-metallic minerals, ores, biomass, and fossil energy carriers from natural deposits into the economy;
* **material production (P2)** transforms raw materials into industry-specific materials using energy and other material inputs;
* **finishing (P3)** and **manufacturing (P4)** transform intermediate materials into generic products;
* **use (P5)** adds products to material stocks and generates material outflows;
* **waste collection (P6)** gathers products leaving the use phase;
* **waste preparation (P7)** sorts and prepares collected material for recovery;
* **recycling (P8)** converts prepared scrap into secondary material; and
* **final waste treatment (P9)** receives material that is not recycled.

The current model explicitly represents the processes and flows relevant to the included material sectors.
Final waste treatment, including landfilling, incineration, and waste-to-energy conversion, is outside the system boundary of version 1.2.0.

Material demand drives industrial production and, through the production of material products, determines the need for raw-material extraction.
The model combines exogenous and endogenous demand.
Population and GDP drive aggregated product-stock demand in non-power sectors, while the energy-system optimization determines the capacity and material stocks of power-generation technologies.
For non-power products, the current implementation uses calibrated relationships between production, waste, and material stocks rather than a fully lifetime-resolved cohort model.

Trade is represented for semi-finished and finished steel, aluminum, and chemical products through a global commodity pool.
The methodology follows the approach used in MESSAGEix-GLOBIOM-GAINS for energy commodities like coal and oil.
# TODO: add cross reference to the trade section of the global model documentation.
That means that in its default configuration trade is frictionless and does not resolve bilateral flows.
Export capacity is represented by a pseudo-technology with investment costs for establishing capacity and variable costs for shipping.
The base-year trade of each region is either bound to COMMTRADE data or endogenously determined by production and consumption data.
Future trade is then allocated by the cost-minimizing solution, subject to the available capacities and policy constraints.

The use phase distinguishes two types of material stocks.
For power generation, stock accumulation is endogenous: new capacity is multiplied by technology-specific material intensities, and retirement releases the associated material flows.
Retirement does not imply that all of this material becomes usable secondary material; the amount recovered depends on collection and recycling assumptions and on process losses.
For other products, stock and waste quantities are represented in an aggregated manner using GDP- and population-driven demand, with calibrated waste ratios distinguishing the portion entering the waste system from the portion remaining in use.

For metals, manufacturing generates new scrap, which can be used directly in manufacturing, and the end of product use generates old scrap.
Steel and aluminum old scrap are represented with three quality levels.
Collection can be subject to minimum rates based on historical or regulatory assumptions, while maximum recycling rates represent technical limitations such as contamination and alloying.
Lower-quality scrap requires more energy-intensive and costly preparation.
Recycling converts prepared scrap into secondary material and accounts for losses during the process.

Version 1.1.0 has established a proof-of-concept implementation of this integrative system definition.
For metals and non-metallic minerals, the raw-material extraction stage is not fully represented: the detailed energy use, physical losses, and waste associated with mining gross ores are omitted.
Instead, raw materials are assigned base-year market prices with a prescribed future price development.
The model therefore prioritizes the energy- and emission-intensive production, finishing, manufacturing, use, and recycling processes.
Comprehensive economy-wide coverage of all materials, end uses, and extraction processes is intended for future versions.

Material supply and processing in reference energy and material system
----------------------------------------------------------------------

Iron and steel
~~~~~~~~~~~~~~

The iron and steel sector represents primary and secondary steelmaking.
Iron ore is used as the primary input for steel production, while scrap
provides a secondary input.
The production chain includes ironmaking, steelmaking, casting, finishing,
and manufacturing.
The technology portfolio includes blast-furnace and basic-oxygen-furnace
routes, electric-arc-furnace routes, direct reduction using natural gas or
hydrogen, charcoal-based production, top-gas recirculation, and
carbon-capture options.
Scrap availability is exogenously defined to constraint future secondary supply.
The model also represents new scrap from manufacturing and old scrap from
products leaving the use phase.

.. _fig-rms-steel:

.. figure:: /_static/RMS_steel.png

   :width: 800px

Cement and non-metallic minerals
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The cement sector distinguishes clinker production from cement blending.
Limestone is the principal material input to clinker production.
The representation includes process heat, alternative fuels, clinker
substitution, and carbon-capture options where parameterized.
This distinction separates process emissions from emissions associated with the energy used for production.
Cement production also includes the blending of clinker with other
materials to produce cement.
End-of-life flows and recycling are not explicitly represented for cement in
version 1.1.0.

.. _fig-rms-cement:

.. figure:: /_static/RMS_cement.png

   :width: 800px

Aluminum
~~~~~~~~

The aluminum sector represents primary production from bauxite through
refining and smelting, as well as secondary production from recycled
aluminum.
Electricity is a major input to primary aluminum production.
Electricity use is an important link to the energy system.
Regional production and trade are calibrated using material-flow data.
The model represents aluminum trade at the product level and includes
manufacturing scrap and old scrap from products reaching the end of their use
phase.
Recycling requires collection and scrap preparation, with energy and costs
that vary by scrap quality.

.. _fig-rms-alu:

.. figure:: /_static/RMS_Aluminum.png

   :width: 800px

Chemicals
~~~~~~~~~

The chemical sector represents ammonia fertilizer, methanol, and high-value
chemical (HVC) production.
The technologies connect chemical production to electricity, heat, hydrogen,
fossil feedstocks, and non-energy use of fuels.
Other chemical activities remain in the generalized industry representation.

.. todo:: Insert Figure 6 from Ünlü et al. (2024), showing the reference
   energy system for the extended refinery representation.

High-value chemicals
^^^^^^^^^^^^^^^^^^^^

The HVC representation covers ethylene, propylene, benzene, toluene, and
xylene.
These products are produced through conventional petrochemical routes using
oil, gas, and ethane feedstocks, or through methanol-to-olefins pathways.
The latter provides a route to link HVC production to alternative methanol
feedstocks, including biomass-based production.
Trade is represented for chemical products.

.. _fig-rms-hvc:

.. figure:: /_static/Chemical_sector_HVC.png

   :width: 800px

Ammonia
^^^^^^^

Ammonia production is linked to fertilizer demand.
The main production routes use hydrogen and nitrogen, with hydrogen supplied
from fossil or low-carbon pathways.
Carbon capture is represented for ammonia production and can use the
relatively concentrated process CO2 stream.
Ammonia is treated as a dissipative product rather than an in-use material
stock.

.. _fig-rms-nh3:

.. figure:: /_static/Chemical_sector_ammonia.png

   :width: 800px

Methanol
^^^^^^^^

Methanol can be produced from fossil feedstocks, biomass, or captured carbon with hydrogen.
The sector distinguishes methanol used as an energy carrier from methanol
used as a chemical feedstock.
Methanol is also an intermediate for the methanol-to-olefins route and can be
produced with carbon capture, including biomass-based routes with the
potential for negative emissions.

.. _fig-rms-methanol:

.. figure:: /_static/Chemical_sector_revised_methanol.png

   :width: 800px

Demand modelling
----------------
The shift from energy demand to material demand requires methodology to project future regional material demand.
Due to data constraints and structural differences between the represented sectors, the model uses different approaches for the four material-intensive sectors.
All methods share the principle of linking to socio-economic drivers directly or via energy dynamics.
The system boundaries for the demand projections are defined to represent the whole economy and do not differentiate between different consumption sectors.


Steel, cement and aluminum
~~~~~~~~~~~~~~~~~~~~~~~~~~

The final material demand for steel and cement is adopted from van Ruijven et al. (2021 :cite:`van_ruijven_2021_global`).
This approach uses regional population and GDP projections to estimate future demand for steel and cement using a functional form that is calibrated to historical data.
The same method has also been applied to aluminum demand, using historical demand data from the International Aluminium Institute.
All regions are assumed to follow the relationship between demand and GDP per capita that was fitted to full sample.
The regional residuals in the base year are slowly converged to zero.

NOTE:
For steel the method has been refined in v.1.2.0 to replace GDP per capita with gross fixed capital formation per capita data as an explaining variable for the demand projection.
The future gross fixed capital formation is determined by the MACRO model.
TODO: cross-ref to the MACRO chapter.


Chemicals
~~~~~~~~~
For ammonia a combination of exogenous and endogenous demand implementation is used.
A large share of ammonia is used for nitrogen fertilizer production, which is projected by the GLOBIOM model.
In the MESSAGEix-Materials-GLOBIOM linkage, the fertilizer demand of the optimal GLOBIOM pathway is passed to MESSAGEix-Materials.
The residual ammonia demand outside of fertilizer production is determined from historical ammonia demand data and projected using scenario results from the IEA Future of Petrochemicals analysis.

For methanol, a similar combination of linkages is implemented.
Methanol demand can be broadly categorized into three groups:

  - methanol used as a traditional chemical feedstock
  - methanol used as an energy carrier
  - methanol used as an intermediate for the methanol-to-olefins route.

The first category is covered by the IEA Future of Petrochemicals scenario.
The second category is projected by linkages to the transport sector in MESSAGEix-GLOBIOM-GAINS.
Since methanol is mainly used as a chemical feedstock for various fuel blending components,
light oil demand for transport was chosen as one driver for methanol demand through an input intensity coefficient.
This coefficient has been regionally differentiated to reflect blending legislation and practices and match historical methanol demand data.
In addition, a substitution with ethanol is implemented to allow for a phase-out of methanol.

The third category is driven by the demand for high-value chemicals and the endogenously determined cost-effectiveness compared to conventional HVC production.

HVCs
~~~~

Due to missing historic consumption data for HVCs, the methodology used for steel, cement, and aluminum could not be applied.
Instead, the IEA Future of Petrochemicals scenarios were used to inform the future demand for HVCs.
The implicit elasticity of demand with respect to GDP per capita has been derived from the IEA scenarios.
The elasticity is then applied to the GDP per capita projections of the MESSAGEix-GLOBIOM-GAINS scenario to project future HVC demand.


Power-sector material requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to the economy-wide material projections, power sector material demands are endogenized in MESSAGEix-Materials.
Material intensities are assigned to MESSAGE power technologies for construction and end-of-life.
Technology-specific life-cycle inventory coefficients are mapped to MESSAGE technologies, regions, material commodities, and model years.
Consequently, changes in the generation mix affect both energy-system emissions and the material requirements of the energy transition.

Residual industry
~~~~~~~~~~~~~~~~~

The energy demand of industrial activities that is not covered by the explicit material sectors is kept in the generalized industry representation of MESSAGEix-GLOBIOM-GAINS.
To account for the explicit, endogenous energy demand, the residual industry demand had to be adjusted to avoid double counting.
A updated projection method was used to generate consistent pathways.


TODO: cross link energy demand chapter
This method is based on the original end-use demand projection method of MESSAGEix-GLOBIOM-GAINS, which uses quantile regression of historical energy demand data against population and GDP projections.
As previously, IEA energy balances were used as a data basis for historical energy demand, but adjusted to only include industry sectors that are not covered by the explicit material sectors.
While the previous regression used the sectoral shares of total end-use energy demand as the dependent variable, the updated method uses the absolute energy intensity of the residual industry as the dependent variable.
Since industrial energy demand is split into process heat (substitutable) and specific (non-substitutable) energy demand, the regression is performed separately for each of these two categories.
As in (original chapter link), a convergence quantile was selected per SSP scenario to ensure that the projected energy demand of the residual industry is consistent with the scenario narrative.

Emissions and environmental impacts
====================================

# TODO: cross reference to the emissions section of the global model documentation.
The emission accounting of MESSAGEix-Materials is integrated with the emissions accounting of MESSAGEix-GLOBIOM-GAINS and follows the same principles.
Material production emissions are therefore assessed together with emissions from the rest of the energy system.
The resulting emissions can be constrained by climate policy and passed to the other components of the MESSAGEix-GLOBIOM-GAINS framework, including MACRO and the climate calculations described in :ref:`overview`.

Carbon-dioxide (CO2)
--------------------
Emission intensities from fuel combustion in industrial processes are defined by the default fuel emission factors (Table 25 in global docs) and the fuel intensity of the processes.
Process emissions that do not stem from fossil fuel oxidation (e.g. limestone calcination in cement clinker production) are represented with technology-specific coefficients derived from literature.
Carbon capture can reduce the emission intensity assigned to the relevant production routes where a capture technology is available.

Non-CO2 greenhouse gases (GHGs)
-------------------------------

Pollutants
----------


Calibration and data
====================

Base-year activities and capacities are calibrated to historical energy, material, production, trade, and stock data.
The model uses sector-specific data sources, including IEA energy balances, World Steel Association statistics, International Aluminium Institute material-flow data, and data sets described in the paper and supplementary information.

The scenario build adds common material structure and then sector-specific data for generalized industry, aluminum, methanol, ammonia, generic technologies, steel, cement, petrochemicals, and optionally the power sector.
The available sectors and technologies are determined by the scenario
specification.


Further reading
===============

The model formulation, data sources, sectoral technology assumptions, and validation are described in Ünlü et al. (2024 :cite:`unlu_2024_materials`).
For the surrounding energy, land-use, emissions, and macro-economic representations, see the :doc:`global model documentation </global/index>`.
