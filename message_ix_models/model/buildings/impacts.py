"""Building energy CID integration point.

Consumes ``predict_rime(gmt, "EI_cool")`` and ``predict_rime(gmt, "EI_heat")``
at R12 native resolution (12 regions x arch x urt x n_years).
No resolution expansion needed.

Domain logic (future):
- Energy decomposition: E = gamma * EI(GSAT) * F
- gamma: correction coefficients (non-climate drivers)
- F: STURM floor area projections
- Sector fraction decomposition of rc_spec / rc_therm demands
- Post-hoc demand replacement (after STURM convergence loop)

See ``project.alps.replace_building_cids`` for the current implementation.
"""
