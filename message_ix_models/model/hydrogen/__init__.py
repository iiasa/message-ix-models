"""Hydrogen technology module for MESSAGE-IX models.

This module provides hydrogen production, storage, and utilization technologies
for MESSAGE-IX scenarios. It includes:

- Electrolyzer technologies (alkaline, PEM, SOE, pyrolysis)
- Hydrogen combined cycle power generation
- Carbon black storage and transport
- Historical calibration and constraints

The main entry point is the :func:`.build` function which adds all hydrogen
technologies to a scenario.
"""

from .build import build, make_spec
from .data_hydrogen import gen_data_hydrogen

__all__ = [
    "build",
    "make_spec", 
    "gen_data_hydrogen",
]

