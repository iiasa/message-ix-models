.. _gains:

CH4, N2O and air pollution (GAINS)
----------------------------------

GAINS, the two ways it is linked into the integrated assessment framework,
and the way abatement options enter the optimization
are introduced in the :ref:`overview`,
and the linkage for air pollutants and the range of control ambition
in the :ref:`policy_overview`.
This page adds the detail behind them.
CH4 and N2O emissions from agriculture and land use come from GLOBIOM
(see :ref:`emission_land`),
and F-gases are represented directly in |MESSAGEix|
(see :ref:`emission_energy_nonco2`).

.. _emission_nonco2:

CH4 and N2O
~~~~~~~~~~~

Drivers of the implied emission factors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The implied emission factors derived from GAINS
cover the full energy chain
as well as non-energy industry, product use, waste and wastewater.
They represent the emissions a process releases to the atmosphere
under the development of regulation assumed in each SSP narrative,
starting from current legislation (CLE),
under which only policies that have already been adopted apply.
Wherever possible, each factor is attached to a driver in |MESSAGEix|,
so that the activity of that driver determines the absolute emissions endogenously.
Emissions are represented in four ways.

* In most cases, the implied emission factor is attributed directly to a technology.
  Some of these factors are mode specific,
  varying with the input fuel or the combustion process,
  so that the activity of each technology mode determines the resulting emissions.
* Where emissions are not tied to a specific combustion process,
  a sectoral demand acts as a proxy for the growth of the emission source.
  Industrial wastewater emissions, for example,
  are driven by industrial thermal demand.
* Exogenous drivers from the SSP narratives, such as GDP or population,
  are used in the same way as sectoral demands.
  Urban and rural population, for example,
  determines domestic wastewater emissions.
* Where |MESSAGEix| has neither a matching technology nor a suitable driver,
  the GAINS emission trajectory is embedded in the model as a fixed trajectory.
  This applies, for example, to CH4 from abandoned coal mines,
  which cannot be determined endogenously.

.. _gains_abatement:

Abatement options
^^^^^^^^^^^^^^^^^

Abatement options for CH4 and N2O are tied to the activity of its parent technology,
the technology or driver with which the implied emission factor is associated,
and reduces that implied emission factor at a given cost.
CH4 emissions from gas distribution, for example,
can be reduced by replacing existing pipelines with polyethylene or polyvinyl chloride pipes,
or by leak detection combined with repair programmes.

.. _gains_deployment:

Abatement deployment across the narratives
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

How much of the abatement potential can be deployed
is limited by both the scenario stringency and the SSP narrative
(:numref:`fig-gains-abatement-deployment`).
Deployment is expressed relative to the maximum technically feasible reduction (MTFR),
the reduction reached when all technically available options are applied in full.
In LED and SSP1,
deployment reaches 60% of the MTFR by 2030 and the full MTFR by 2070.
SSP2 also reaches 60% by 2030,
but its long-term deployment is capped at 70%,
reflecting the expectation that only the lower-cost options are taken up.

For CH4,
most of the deployment up to 2030 takes place in the energy sector.
The most important measures are the extended recovery of associated petroleum gas during oil extraction
and regenerative thermal oxidation of the ventilation air from underground coal mines.
A long-term cap at 70% exhausts the low-cost CH4 abatement potential.
The waste sector contributes little to CH4 mitigation by 2030,
but considerably more by 2070.
Its near-term potential is limited
because organic waste already deposited in landfills
continues to decompose and release CH4.
Provided organic waste is diverted from landfills to circular waste systems early on,
the waste sector holds a large CH4 mitigation potential in the long term.

The remaining narratives are more constrained.
SSP3 and SSP4 deploy no abatement by 2030
and reach 20% of the MTFR by 2100,
reflecting weak international cooperation.
SSP5 allows more deployment on account of higher incomes,
reaching 50% of the MTFR by 2100,
despite its fossil-fuel-based economy.

.. _fig-gains-abatement-deployment:
.. figure:: /_static/gains_ch4_n2o_abatement_deployment.png
   :width: 800px
   :align: center

   Maximum CH4 and N2O abatement deployment by narrative, as a share of the maximum technically feasible reduction (MTFR). Each line rises from zero in 2025 to its 2030 share, then ramps linearly to its maximum share and holds constant. LED coincides with SSP1 and SSP3 coincides with SSP4, so the LED and SSP3 lines are hidden (Fricko et al., 2026 :cite:`fricko_wu_2026`).

.. _gains_airpollution:

Air pollutants
~~~~~~~~~~~~~~

Revisions of the GAINS database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Besides the review of current legislation described in the :ref:`policy_overview`,
the GAINS database was revised for major industrial and energy sources.
Updated information on non-ferrous metal production, power plants and industrial boilers,
complemented by satellite observations and new inventory data,
improves the estimates of SO2, NOx and PM emissions across a wide range of countries.
Historical and projected emissions were updated using recent inventories
of the Community Emissions Data System (CEDS)
(Hoesly et al., 2018 :cite:`hoesly_ceds_2018`)
and regional studies,
which improves the consistency between GAINS and CEDS.

Further revisions cover residential combustion, transport, waste management and agricultural burning.
Updated fuel use patterns, heating technologies and household energy data
improve the estimates of PM and black carbon (BC) emissions,
particularly in South Asia, the Western Balkans, Central Asia and ASEAN.
Transport assumptions reflect updated fuel quality standards,
vehicle emission regulations and fleet turnover.
New information on waste management and open agricultural burning
further improves the estimates of PM and BC emissions.
Together, these updates define the CLE scenario in GAINS,
the reference level of pollution control from which the narratives depart.

.. _gains_narratives:

Air pollution narratives
^^^^^^^^^^^^^^^^^^^^^^^^

Air pollution control assumptions differ across the narratives,
following the governance capacity and the level of international cooperation of each SSP.
Control ambition ranges from CLE up to the MTFR.
For very low emission pathways,
all regions move after 2070 toward a global technological frontier,
defined by the lowest region-specific MTFR level.
Details of the air pollution narratives and the SSP variants
are provided by Zhang et al. (2026) :cite:`zhang_2026_air_pollution`.

SSP1 (Sustainability)
"""""""""""""""""""""

SSP1 assumes a progressive tightening of air pollution control policies,
reaching the MTFR by 2070.
This is an ambitious but realistic policy trajectory,
in line with growing international commitments to improve air quality,
while recognizing constraints on the pace of regulatory implementation and technology deployment.
The Very Low variant of SSP1 goes beyond the MTFR
by moving all regions toward the global technological frontier,
defined by the best available technology achieved in any region.
This implies an exceptionally rapid diffusion of the best available technologies,
strong international policy coordination,
and near-complete harmonization of air pollution control performance across countries.

LED (Low Energy Demand)
"""""""""""""""""""""""

LED shares the air pollution assumptions of SSP1,
reaching the MTFR by 2070.

SSP2 (Middle of the Road)
"""""""""""""""""""""""""

SSP2 assumes a moderate strengthening of air pollution policies.
By 2100, control levels reach a blend of about three quarters CLE and one quarter MTFR.
Emissions decline considerably but do not reach the MTFR.

SSP3 (Regional Rivalry) and SSP4 (Inequality)
"""""""""""""""""""""""""""""""""""""""""""""

In SSP3 and SSP4,
weak institutional development keeps air pollution control close to CLE,
so that emission reductions are slower and less consistent than in SSP2.
Limited gains in governance capacity delay the adoption of the best available technologies,
leading to persistent differences in emission control performance across regions and sectors.

SSP5 (Fossil-fueled Development)
""""""""""""""""""""""""""""""""

SSP5 adopts air pollution policy assumptions similar to those of SSP1,
reaching the MTFR by 2070.
