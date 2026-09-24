.. _co2removal_methods:

CO2 removal (CDR) methods
-------------------------

In MESSAGE, a range of technology-based CO2 removal approaches are represented. This representation includes bioenergy with carbon capture sequestration (BECCS), 
direct air carbon capture and sequestration (DACCS), biochar for non-land-use applications, biomass burial, enhanced rock weathering (ERW), and ocean alkalinity enhancement (OAE).

CO2 Transport, Utilisation, and Storage Infrastructure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The availability of carbon dioxide transport, utilisation, and storage (TU&S)
infrastructure is essential for CO2 capture technologies. In the previous
MESSAGEix models implementation, this infrastructure was represented only
through variable costs approximating industry transport and storage tariffs,
with the assumption that all captured CO2 would be permanently stored in
geological formations. The updated implementation represents the CO2
supply chain explicitly by introducing captured CO2 as a commodity and
representing CO2 transmission pipelines, geological storage, and utilisation
as separate processes. This allows captured CO2 to be either permanently
stored or supplied to industrial applications rather than assuming that all
captured CO2 is stored.

CO2 captured by fossil and industrial-process CCS (FICC), biomass-based
technologies, and direct air capture (DAC) is transported to either geological
storage sites or CO2 utilisation facilities. The model represents CO2
transport exclusively through pipelines, reflecting the expected importance
of pipelines for large-scale deployment of CCS-based mitigation options. Two
pipeline-distance categories are represented to capture differences in the
proximity between CO2 capture facilities and storage or utilisation sites and
the associated transport costs. DAC is assumed to use short-distance
pipelines because of its greater siting flexibility. In contrast, BECCS is
constrained by biomass availability and the location of biomass resources and
is therefore assumed to use only long-distance pipelines shared with fossil
and industrial-process CCS facilities. These pipelines can connect CO2
capture facilities to either geological storage sites or utilisation plants.

.. figure:: /path/to/co2_tus_infrastructure.png
   :alt: Structure of carbon capture technologies and associated CO2 transport, utilisation, and storage infrastructure

   Structure of carbon capture technologies and associated CO2 transport,
   utilisation, and storage infrastructure.

Geological CO2 storage is constrained by both cumulative storage capacity and
annual injection rates. The cumulative amount of CO2 that can be stored in
each region is limited using the prudent-limit estimates from (Gidden et al, 202X). In
addition, annual CO2 injection rates are constrained based on estimates from
[CITATION]. These injection-rate constraints apply to the total amount of CO2
injected into geological storage across all capture technologies and therefore
capture competition for storage capacity is represented explicitly. In
contrast, the CO2 utilisation segment is not subject to an explicit resource
constraint. Instead, demand for CO2 utilisation is determined endogenously
within the model.

The explicit representation of CO2 as a commodity also allows the model to
track the fate of captured CO2 throughout the TU&S system. CO2 accounting is
attributed to the pipeline, geological storage, and utilisation processes,
allowing the model to account for CO2 that is leaked during transport or
storage, permanently stored, or released back into the atmosphere following
its use as an industrial feedstock. An exception is made for carbon
incorporated into plastics, for which the carbon is assumed to remain stored.
This representation therefore distinguishes between captured CO2 that is
transported, stored, utilised, leaked, or ultimately released, rather than
treating all captured CO2 as permanently removed from the atmosphere.

Bioenergy with CCS (BECCS)
~~~~~~~~~~~~~~~~~~~~~~~~~~

[Need to check if there is any documentation from legacy BECCS implementation]


Direct Air Capture (DACCS)
~~~~~~~~~~~~~~~~~~~~~~~~~~

In MESSAGEix, we represent three distinct DAC technology configurations, namely `dac_hte`, `dac_htg`, and `dac_lt`, which differ in their CO₂ capture and regeneration processes and in the energy sources used to meet their heat requirements. 
The high-temperature configurations, `dac_hte` and `dac_htg`, use aqueous solutions to capture CO₂ and require high-temperature regeneration. 
Here, `dac_hte` uses electricity from the grid to meet its heat requirements, whereas `dac_htg` uses natural gas to generate the high-temperature heat required for pellet regeneration. 
In contrast, `dac_lt` uses low-temperature solid sorbents for both CO₂ capture and regeneration, with the lower heat requirements assumed to be met using electrically powered heat pumps. 
These technology configurations and their underlying assumptions are based on the literature.

All CO₂ captured by carbon-capture technologies, including DAC, is represented in the model as a CO₂ commodity. CO₂ captured through DAC is represented by the commodity `dac_CO2`, which serves as an input to CO₂ transport infrastructure. 
The captured CO₂ can subsequently be transported either to geological storage for permanent storage or to industrial plants for use as a feedstock. 
Infrastructure requirements for DAC are therefore represented through the common CO₂ transport and storage infrastructure used by other CCS-based technologies.

The current implementation assumes no additional direct emissions associated with DAC activities. 
Other CO₂ abatement technologies in the model, including CO₂ capture and scrubbers for fossil fuels, industrial processes, and biomass, account for associated non-CO₂ GHG emissions; no additional emissions are currently assigned to DAC. 
Similarly, environmental impacts such as chemical pollution, land and water footprints, and toxicity are not yet explicitly represented for DAC, although such impacts are included for other technologies in MESSAGEix-GLOBIOM. 

In MESSAGEix, DAC captures CO₂ immediately, and therefore removal-timing parameters do not apply to this technology in the model. 
Permanence is represented through the CO₂ transport and storage infrastructure rather than being assigned directly to DAC. 
We assume a CO₂ leakage rate for both transport and storage, represented by the `CO2_Emission` relation activity parameter, which is used for top-down CO₂ accounting in the model.

The deployment of DAC is constrained by both technology-specific and method-specific dynamic growth rates. 
Technology-specific constraints apply to the individual DAC configurations (`dac_lt`, `dac_htg`, and `dac_hte`), while method-specific constraints apply to total DAC deployment. 
The latter are represented using `DAC_mpen` as a pseudo-technology to capture the aggregate market penetration of all DAC technologies. 
In addition, all CO₂ capture-based technologies are subject to common storage availability constraints. 
CO₂ captured by fossil and industrial CCS, biomass CCS, and DAC is assumed to be stored exclusively in saline formations, with storage availability varying by region. 
Other types of geological CO₂ storage, including depleted oil and natural gas reservoirs and unmineable coal seams, are not represented. 
Global CO₂ injection-rate constraints further limit the total annual amount of CO₂ injected from all capture technologies.

The technical performance of DAC—including capacity factor, energy intensity, and technical lifetime—is assumed to be consistent across regions in MESSAGEix. 
Technology costs and allowable capacity growth, however, vary by region. 
DAC technology costs are indexed to the costs of analogous technologies and evolve over time according to assumed cost reductions and GDP.


Biochar for non-land-use applications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Biochar is pyrogenic carbon produced by the incomplete combustion of plant
biomass. It can be used as a carbon dioxide removal (CDR) method by storing
biogenic carbon over long periods, due to substantial reductions in
decomposition and chemical transformation rates.

In MESSAGEix, biochar production is represented using fast and slow
pyrolysis as technology archetypes. These methods differ primarily in the
composition of their outputs, which include biochar, biogas, and bio-oil.
Several technology configurations for producing biochar have been identified
in the literature. To simplify their representation in the model,
all biogas and bio-oil by-products are assumed to be combusted to produce
either high-temperature heat or electricity. The resulting CO2 emissions can
either be captured, providing additional CO2 removal, or released to the
atmosphere. Combining the two pyrolysis processes with the two possible energy
outputs and the presence or absence of a capture unit results in eight
technology archetypes:

.. list-table:: Technology characteristics for biochar production
   :header-rows: 1
   :widths: 30 20 20 20

   * - Technology
     - Pyrolysis process
     - Energy output
     - Capture unit
   * - ``biocharheat_pyros``
     - Slow
     - Heat
     - No
   * - ``biocharelectr_pyros``
     - Slow
     - Electricity
     - No
   * - ``biocharheat_pyrof``
     - Fast
     - Heat
     - No
   * - ``biocharelectr_pyrof``
     - Fast
     - Electricity
     - No
   * - ``biocharheat_pyros_ccs``
     - Slow
     - Heat
     - Yes
   * - ``biocharelectr_pyros_ccs``
     - Slow
     - Electricity
     - Yes
   * - ``biocharheat_pyrof_ccs``
     - Fast
     - Heat
     - Yes
   * - ``biocharelectr_pyrof_ccs``
     - Fast
     - Electricity
     - Yes

The biochar supply chain represented in MESSAGEix consists of
pyrolysis, biochar transport, and biochar application. Biochar produced by the
eight pyrolysis configurations is transported to application sites using the
``bc_trans`` technology. Two types of biochar application are represented in
the model through separate modes of the ``biochar_apply`` technology:
permanent removal and other applications.

.. figure:: /path/to/biochar_supply_chain.png
   :alt: Biochar for non-land-use applications supply chain structure in MESSAGEix

   Biochar for non-land-use applications supply chain structure in MESSAGEix.

As a biomass-based CDR method, biochar production uses biomass from the
GLOBIOM land-use model as its input commodity. Pyrolysis plants are assumed to
be self-sustaining, meeting their energy requirements from the biomass input
and using a portion of the electricity they generate. Biochar transportation
uses diesel (``lightoil``) as an energy input. The resulting biochar is then
supplied to the ``biochar_apply`` technology for use in concrete aggregates or
other applications.

Pyrolysis primarily produces biochar, which is subsequently used in concrete
aggregates and other applications. The process also produces bio-oil and
biogas as by-products. In the model, these by-products are combusted to
produce either high-temperature heat for other industrial processes or
electricity to supply electricity demand. Depending on the technology
configuration, the CO2 generated during combustion can be captured and
represented as a CO2 commodity, which can then be supplied to CO2 transport,
storage, and utilisation infrastructure.

Other emissions associated with biochar production and application are not
explicitly represented in MESSAGEix-GLOBIOM. Likewise, the current
implementation does not account for additional environmental impacts
associated with biochar production or application.

CO2 removal from biochar is assumed to occur immediately when the biochar is
produced. The time required for biomass growth is accounted for within the
GLOBIOM framework and is therefore not represented as an additional timing
parameter in the MESSAGEix-GLOBIOM biochar representation. The model
distinguishes between permanent removal and other applications through
separate modes of the ``biochar_apply`` technology. Permanent removal refers
to long-term carbon storage through the incorporation of biochar into concrete
aggregates. Other non-land-use applications include contaminated-soil
remediation, water filtration, wastewater treatment, and adsorption in
landfills or abandoned mines. These applications are subject to biochar decay,
with annual decay rates explicitly represented in the model based on literature.

Deployment of biochar as a CDR method is subject to several resource and
system constraints. The deployment of biochar for other applications is
constrained by an upper bound on annual growth, implemented through
``biochar_mpen``, which represents regional market penetration. Biochar
deployment is also limited by biomass availability and competition for biomass
resources with other technologies, including BECCS, biomass burial, and
biomass used as a feedstock for industrial processes. Pyrolysis technologies
equipped with carbon capture are additionally constrained by regional
geological CO2 storage availability, since the captured CO2 competes for
storage capacity with CO2 from other carbon-capture-equipped processes.

The application of biochar as a concrete aggregate is constrained by regional
cement demand, which serves as a proxy for the quantity of concrete
production, together with the assumed rate of biochar application in concrete. 
By contrast, biochar used for other applications is not subject to an
explicit application constraint because this category encompasses a wide range
of potential uses.

The technical representation of biochar therefore varies across regions
primarily through resource availability and infrastructure constraints rather
than through different technology configurations. Biomass can be traded
between regions; however, pyrolysis with carbon capture remains constrained by
region-specific geological CO2 storage availability. Similarly, the potential
for biochar application in concrete is limited by annual regional concrete
availability, represented using cement demand as a proxy.


Biomass Burial
~~~~~~~~~~~~~~

Biomass burial is a carbon dioxide removal (CDR) method that removes CO2 from
the atmosphere by physically storing woody biomass underground.
The process involves the handling of biomass and the construction and
operation of burial sites. Compared with other CDR methods, biomass burial
involves a relatively simple process and does not require the representation
of multiple technology configurations. In MESSAGEix-GLOBIOM, it is therefore
represented by a single generic technology, ``biomass_burial``, which captures
the overall process of transporting and applying biomass to burial sites.

Biomass from the GLOBIOM land-use model serves as the main input commodity for
biomass burial. Diesel fuel is also required as an energy input for biomass
burial on croplands. The ``biomass_burial`` technology does not generate
a commodity output; the carbon contained in the biomass is instead accounted
for as a carbon dioxide removal. Other emissions associated with biomass
burial are not explicitly represented in MESSAGEix-GLOBIOM, except for
indirect emissions associated with diesel use. These emissions are accounted
for endogenously through the fuel supply chain represented in the model.
Similarly, no additional environmental impacts associated with biomass burial
are currently considered.

CO2 removal from biomass burial is assumed to occur immediately when the
biomass is applied to the burial site. The time required for biomass growth is
already accounted for within the GLOBIOM framework and is therefore excluded
from the removal timing in MESSAGEix-GLOBIOM. Once the biomass is buried, the
CO2 captured during biomass growth is assumed to remain permanently stored.
Accordingly, permanence is represented directly through the biomass burial
process rather than through a separate transport or storage infrastructure.

The deployment of biomass burial is subject to both market penetration and
resource constraints. The annual growth rate of biomass burial is limited by
an upper bound implemented through ``biomass_burial_mpen``, which represents
biomass burial market penetration in each region. The availability of biomass
and competition for biomass resources with other technologies, including
BECCS and biochar production, further constrain the potential for biomass
burial. In addition, biomass burial is assumed to take place on croplands in
the model, making cropland availability an additional constraint on
deployment.

Biomass can be traded between regions, but cropland availability is
region-specific. Consequently, although the techno-economic parameters of
biomass burial are assumed to be consistent across regions, the cumulative
removal potential represented in MESSAGEix-GLOBIOM varies by region according
to the availability of cropland and biomass resources.

Enhanced Rock Weathering (ERW)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Enhanced rock weathering (ERW), also referred to as enhanced weathering (EW),
is a carbon dioxide removal (CDR) method that accelerates natural mineral
carbonation. The process removes CO2 from the atmosphere through the
weathering of reactive minerals and its subsequent incorporation into
carbonate minerals. The rate at which CO2 is removed depends on factors
including mineral composition, particle size, soil temperature, and soil pH,
with removal occurring over timescales ranging from months to decades.
Because emissions from the supply chain can be significant relative to the
amount of CO2 removed, and therefore affect the overall removal efficiency,
MESSAGEix-GLOBIOM explicitly represents the main processes involved in ERW,
including rock extraction, transportation, grinding, and application to
cropland.

MESSAGEix-GLOBIOM considers basalt and dunite as mineral sources for ERW,
given their relatively widespread availability across global regions. 
The same supply-chain processes are used for both rock types, while
their different mineral compositions affect their energy requirements and the
amount and timing of CO2 removal. The ERW supply chain represented in the model
consists of rock mining, transportation, grinding, and application to
cropland.

.. figure:: /path/to/erw_supply_chain.png
   :alt: Enhanced rock weathering supply chain in MESSAGEix-GLOBIOM

   Enhanced rock weathering supply chain in MESSAGEix-GLOBIOM.

Rock extraction is represented by the ``erw_mining`` technology. It uses
light oil (diesel) as an energy input and produces basalt and dunite as the
primary ERW commodities, represented in the model as ``erw_basalt`` and
``erw_dunite``, respectively. The extracted rocks are then transported from
the quarry to the grinding facility and subsequently to the cropland where
they are applied. These two transportation stages are combined into the
``erw_trans`` technology. The model accounts for variation in the total
transport distance using data from literature, which relates transport distance to
the fraction of cropland area suitable for ERW application. For
computational simplicity, these data are aggregated into five distance bins.

Before application, the rocks undergo particle-size reduction in the
``erw_grind`` technology. The initial particle size of 5,000 µm is reduced to
one of four target particle sizes: 10, 25, 50, or 100 µm. The
particle-size-specific energy requirements for crushing and grinding are
estimated using data from literature. The resulting particle sizes are not assumed
to be uniform. Instead, they are characterized using the P80 diameter, which
is the screen size through which 80% of the sample mass passes. The model uses
the Rosin-Rammler distribution to represent the resulting particle-size
distribution. The processed rocks are subsequently applied to cropland, where
they react with CO2 from the atmosphere and remove carbon through enhanced
weathering.

Basalt and dunite are therefore the primary material inputs to the ERW supply
chain, progressing from the primary resource through mining, transportation,
and grinding to final application on cropland. Light oil (diesel) is used as
an energy input for mining, transportation, and application, while electricity
is used for rock crushing and grinding. Unlike BECCS, ERW does not produce
energy or chemical co-products. The outputs of the supply-chain technologies
are instead the ERW materials at successive stages of processing, with the
final products differentiated by rock type and P80 particle diameter to
capture the effect of particle size on the timing of CO2 removal.

The timing of CO2 removal from ERW is explicitly represented in
MESSAGEix-GLOBIOM. Removal timing accounts for the reaction kinetics of the
minerals contained in the rocks, as well as the soil pH and temperature of the
cropland to which they are applied. Using the reaction kinetic data and soil pH and
temperature data from literature, the spatially-explicit removal timing associated with ERW application in different regions is estimated. 
In our model, CO2 removed through ERW is assumed to be permanently stored.

The current implementation does not assign additional direct emissions to ERW
activities. However, the energy requirements of the ERW supply chain result in
indirect emissions, including non-CO2 GHG emissions, which are accounted for
through the emissions associated with the production and use of the relevant
energy carriers. Other environmental impacts, including chemical pollution,
land and water footprints, and toxicity, are not currently represented in the
ERW implementation. Future developments may incorporate these environmental
considerations.

Deployment of ERW is constrained by both annual growth and the availability
of suitable cropland. To provide a conservative representation, the annual
growth rate of ERW deployment is limited to 10%. The cumulative availability
of basalt and dunite resources is not explicitly constrained, reflecting the
assumed abundance of these minerals. Instead, the amount of rock that can be
applied to cropland is limited to 20 tonnes per hectare per year, as suggested in the literature.

The industrial processes involved in the ERW supply chain are assumed to have
the same techno-economic performance across regions, and no explicit regional
differentiation is applied to these technology assumptions. Nevertheless, the
resulting costs and indirect emissions vary endogenously across regions
according to the energy supply, transport requirements, and other components
of the optimal system configuration. Regional conditions also affect the
timing and potential of CO2 removal: removal timing depends on the soil pH and
temperature of the application area, while the maximum annual quantity of ERW
that can be applied depends on the available cropland area in each region.

Ocean Alkalinity Enhancement (OAE)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ocean alkalinity enhancement (OAE) removes CO2 from the atmosphere by
artificially increasing ocean alkalinity through the addition of alkaline
minerals. This increases the capacity of seawater to take up and store carbon
in the form of bicarbonate (HCO3-) and carbonate (CO3^2-). A range of
minerals can be used for OAE, including naturally occurring silicates and
calcite and anthropogenically produced lime. In MESSAGEix-GLOBIOM,
quicklime spreading (ocean liming) is used as the archetype for OAE.

As with enhanced rock weathering (ERW), OAE involves several supply-chain
processes whose associated energy use and emissions can significantly affect
the net CO2 removal achieved by the technology. The OAE supply chain
represented in MESSAGEix-GLOBIOM therefore includes rock extraction and
primary grinding, transportation, calcination to produce quicklime, and
transport and spreading of quicklime in the ocean.

.. figure:: /path/to/oae_supply_chain.png
   :alt: Ocean alkalinity enhancement supply chain in MESSAGEix-GLOBIOM

   Ocean alkalinity enhancement supply chain in MESSAGEix-GLOBIOM.

The first stage of the OAE supply chain is represented by the ``oae_mining``
technology, which includes the extraction and primary grinding of limestone.
These processes account for a relatively small share of the total energy use
and cost of OAE implementation. The extracted limestone is then transported
to the calcination facility using the ``oae_trans`` technology. The
transportation representation is consistent with that used for ERW, but OAE
is restricted to limestone deposits located within 10 km of the coast. This
reflects the assumed availability of coastal limestone resources, with
approximately 5,000 Gt of limestone estimated to occur beneath bare ground or
scrubland within 10 km of coastlines.

At the calcination stage, limestone is converted into quicklime, which is
subsequently applied to the ocean. Calcination requires high-temperature heat
and produces CO2 both from the calcination reaction and from the combustion of
the fuel used to provide process heat. To improve the net CO2 removal
efficiency of OAE, these emissions are assumed to be captured using carbon
capture and storage (CCS). The final stage is represented by the
``oae_apply`` technology, which transports and spreads quicklime in the ocean
using ships. These ships are assumed to become available from 2030 onward.

The OAE supply chain requires several material and energy inputs. The
``oae_trans`` technology uses limestone, represented by the ``oae_limestone``
commodity, as its material input and diesel, represented by ``lightoil``, as
its energy input. The calcination process requires high-temperature heat,
which is supplied by gas. The gas can be provided entirely by natural gas or
by a blend of natural gas and biogas, with the share of biogas in the mix
determined endogenously by the model. Electricity is also required for plant
operation and equipment. The ``oae_apply`` technology uses quicklime as its
primary material input and consumes diesel, represented by ``lightoil`` at the
final-energy level.

The OAE supply-chain technologies do not generate energy as a co-product.
However, calcination generates CO2 through both the calcination reaction and
fuel combustion. These emissions are captured using CCS and represented as a
CO2 commodity. The captured CO2 is subsequently supplied to the CO2 transport,
utilisation, and storage infrastructure. Other direct emissions from OAE
activities are not explicitly represented. Indirect emissions, including
associated non-CO2 GHG emissions, are instead accounted for through the
emissions associated with the energy inputs used throughout the OAE supply
chain.

The current representation of OAE does not explicitly account for broader
environmental impacts, including chemical pollution, land and water
footprints, or toxicity. Future developments may incorporate these
environmental considerations into the OAE representation.

OAE requires weeks to months to capture CO2 from the atmosphere effectively.
Because this timescale is shorter than the temporal resolution of
MESSAGEix-GLOBIOM, CO2 removal from OAE is represented as occurring
immediately in the model. The resulting CO2 removal is assumed to be
permanent.

Deployment of OAE is constrained by both annual growth and the availability
of key resources and infrastructure. Annual OAE deployment is limited to a
10% growth rate. In addition, the quicklime-based OAE pathway is constrained
by the availability of CO2 storage and limestone. The CO2 generated during
calcination must be captured and stored, and the available storage capacity
is shared with captured CO2 from other sources. Consequently, limitations on
CO2 storage capacity can indirectly constrain OAE deployment. Limestone
availability provides a second constraint: annual limestone excavation is
limited using cement production as a proxy for the amount of limestone that
can be extracted.

The technologies involved in the OAE supply chain are assumed to be
relatively mature, and no regional differentiation is therefore applied to
their techno-economic parameters or assumed future improvements. However,
the constraints governing OAE deployment are region-specific. In particular,
the availability of CO2 storage and the annual limit on limestone excavation
vary across regions and therefore determine the regional deployment potential
of OAE.