"""Cooling CID integration point.

Consumes ``predict_rime(gmt, "capacity_factor")`` at R12 native resolution
(12 regions x n_years). No resolution expansion needed.

Domain logic (future):
- Jones ratio: CF(GMT) / CF_baseline -- warming reduces cooling efficiency
- ``relation_activity`` constraints bounding freshwater cooling activity
- Reads scenario structure: addon_conversion, cooling shares

See ``project.alps.replace_cooling_cids`` for the current implementation.
"""
