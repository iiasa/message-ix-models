Time steps
==========

In |name| the time horizon runs from 1960 to 2110,
in five-year periods to 2060 and ten-year periods thereafter.
Capacity is accounted for back to 1960 and activity data back to 1990,
and 2020 is the last fully calibrated period.
The 2025 period is partly calibrated, with demands and bounds
tightened to recent trends while some flexibility remains,
and because the socio-economic drivers diverge only from 2030 onwards,
its demand trajectories and the parameters behind them
are the same across scenarios.
Results are typically reported for 2020 to 2100.
The reporting years are the final years of periods,
which implies that the investments leading to the capacities
in a reporting year are the average annual investments
over the entire period that year belongs to.

|MESSAGEix| can both operate perfect foresight over the entire time horizon, limited foresight (e.g., two or three periods into the future) or myopically, optimizing one period at a time (Keppo and Strubegger, 2010 :cite:`keppo_short_2010`) (see `Mathematical Specification <https://docs.messageix.org/en/stable/model/MESSAGE/model_solve.html#recursive-dynamic-and-myopic-model>`_ for more details). Most frequently |MESSAGEix| is run with perfect foresight, but for specific applications such as delayed participation in a global climate regime without anticipation (Krey and Riahi, 2009 :cite:`krey_implications_2009`; O'Neill et al., 2010 :cite:`oneill_mitigation_2010`) limited foresight is used.

GLOBIOM models the time horizon 2000 to 2100 in 10 year time steps (2000, 2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100) with the year 2000 being the base year of the model.  The model is recursive-dynamic, i.e. it is solved for each period individually and then passes on results to the subsequent periods. The linkage between |MESSAGEix| and GLOBIOM relies on the model results of the periods 2020 to 2100.
