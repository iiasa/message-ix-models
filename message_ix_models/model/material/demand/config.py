from dataclasses import dataclass
from typing import Callable, List


@dataclass
class FittingConfig:
    function: Callable
    initial_guess: List[int]
    x_data: List[str]
    phi: int
    mu: float
