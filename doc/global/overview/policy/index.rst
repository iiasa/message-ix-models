.. _policy_overview:

Policies
========
A number of different energy- and climate-related policies are,
depending on the scenario setup and the research question addressed,
explicitly represented in |name|.
This includes the following list of policies:

* GHG emission pricing
* GHG emission caps and trading emission allowances
* Currently implemented policies, targets pledged under the Nationally
  Determined Contributions, and national net-zero targets
* Renewable energy portfolio standards (e.g., share of renewable energy in electricity generation)
* Renewable energy and other technology capacity targets
* Energy import tariffs
* Fuel subsidies and micro-financing for achieving universal access to modern
  energy services in developing countries
  (Poblete-Cazenave et al., 2021 :cite:`poblete_2021_scenarios`)
* Air pollution control, ranging from current legislation to the maximum
  technically feasible reduction, via linkage to the
  `GAINS model <https://iiasa.ac.at/models-tools-data/gains>`_
* Abatement of methane and nitrous oxide, also via the GAINS linkage

Explicit representation of mitigation policy replaces the Shared Policy Assumptions
(SPAs) of the previous scenario generation developed for ScenarioMIP-CMIP6
(Kriegler et al., 2014 :cite:`kriegler_2014_spa`),
which no longer serve on their own as a contextualisation layer
now that near-term pledges and national net-zero targets are concrete.

Near-term policy ambition is instead represented following the methodology of
Rogelj et al. (2017 :cite:`rogelj_indc_2017`),
which differentiates levels of ambition
and applies national net-zero targets.
Currently implemented policies can be taken from the Climate Policy Database
(Nascimento et al., 2022 :cite:`nascimento_2022_twenty`,
NewClimate Institute et al. :cite:`climate_policy_database`),
counted as of a chosen year
and excluding pledges that are not yet legislated.
Pledged targets are taken from the Nationally Determined Contributions
submitted to the `UNFCCC NDC registry <https://unfccc.int/NDCREG>`_,
and can be applied at different stringencies,
for instance unconditional pledges only,
or including those conditional on international climate finance.
Individual national targets can be excluded
where their achievement is judged implausible,
and that judgement can be carried forward
into the timing of the national net-zero targets that follow from them,
subject to the scenario analysis being undertaken.
Beyond the period a near-term policy covers,
the effort can be extrapolated through a carbon price
consistent with the emission reductions it achieves
relative to a counterfactual baseline.
The categories of target the model translates into constraints,
and the constraints themselves, are described in :ref:`policy`.

Air pollution policies are represented through the linkage to the
`GAINS model <https://iiasa.ac.at/models-tools-data/gains>`_
(Amann et al., 2011 :cite:`amann_cost-effective_2011`).
The current-legislation baseline in GAINS is reviewed continuously
and updated as new regulation is enacted,
drawing on national assessments and recent international studies
(Purohit et al., 2026 :cite:`purohit_2026_uttar_pradesh`;
World Bank, 2025 :cite:`world_bank_2025_clean_air`;
Klimont et al., 2025 :cite:`klimont_2025_gridded`;
European Commission et al., 2025 :cite:`ec_2025_cao4`).
The linkage runs downstream of the optimization
rather than entering it as emission coefficients.
Scenario outputs from |name| are harmonized with GAINS
through a trend-preserving downscaling framework,
which retains the sector, activity and technology resolution of GAINS
while aligning its activity trajectories with the scenario.
A GAINS pattern scenario is first initialized at native resolution,
a sectoral and regional mapping between the two models is established,
the scenario outputs are downscaled onto that resolution,
and a post-processing step restores internal consistency
of heat balances, fuel accounting and process activities.
Emissions are then computed within GAINS
and aggregated back to IAMC definitions on the |name| regions.

The level of air pollution control applied follows the SSP narratives
rather than a fixed set of legislation packages.
Ambition ranges from current legislation (CLE),
under which only already-adopted policies apply,
up to the maximum technically feasible reduction (MTFR),
see :ref:`air pollution <gains>`.

Methane and nitrous oxide reach the model through the same linkage.
Emission factors derived from GAINS are attached to source-specific drivers,
whether technology activity, delivered demand, or population and income,
and follow the GAINS source sectors for the two species.

GAINS also supplies the corresponding abatement options
at the same sectoral and technological resolution,
tied to the activity of the parent technology
with which the emission factor is associated.
Unlike a conventional marginal abatement cost curve,
this adds explicit technologies,
so that the associated energy requirements or energy recovery,
lifetimes and detailed costs are represented.

How far those options are deployed is itself a policy assumption,
and it differs across the SSP narratives in both the near and the long term,
see :ref:`non-CO2 GHGs <emission_nonco2>`.

The model linkage, the air pollution narratives and the abatement assumptions
are described in full in a companion paper in this special issue
(Zhang et al., in preparation :cite:`zhang_2026_air_pollution`).
