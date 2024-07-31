from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

from genno import Key

from .key import pdt_cap

if TYPE_CHECKING:
    import genno
    from genno.core.key import KeyLike

    from message_ix_models import Context

#: List of all :class:`.ExogenousDataFile`.
FILES: List["ExogenousDataFile"] = []


class ExogenousDataFile:
    """Information about exogenous data expected in a file."""

    #: Parts of the path.
    parts: Tuple[str, ...]

    #: Key, including preferred dimensions.
    key: Key

    #: Documentation/description of the data flow.
    doc: Optional[str]

    required: bool = True

    def __init__(
        self,
        parts: Union[str, Tuple[str, ...]],
        key: Optional["KeyLike"] = None,
        doc: Optional[str] = None,
        required: bool = True,
        *,
        dims: Optional[Tuple[str, ...]] = None,
    ):
        self.parts = (parts,) if isinstance(parts, str) else parts
        self.parts = self.parts[:-1] + (self.parts[-1] + ".csv",)
        self.doc = doc
        self.required = required

        if not key:
            # Determine from file path
            name = " ".join(Path(*self.parts).with_suffix("").parts).replace("-", " ")
            self.key = Key(name, dims or (), "exo")
        else:
            # Convert to Key object
            self.key = Key(key)
            if dims:
                assert dims == self.key.dims

        # Add to the list of FILES
        if not any(f.key == self.key for f in FILES):
            FILES.append(self)

    # Does nothing except ensure callable(…) == True for inspection by genno
    def __call__(self): ...

    def __repr__(self) -> str:
        return f"<ExogenousDataFile {'/'.join(self.parts)} → {self.key}>"

    def add_tasks(
        self, c: "genno.Computer", *args, context: "Context"
    ) -> Tuple["KeyLike", ...]:
        from message_ix_models.util.ixmp import rename_dims

        from .util import path_fallback

        try:
            path = path_fallback(context, *self.parts)
        except FileNotFoundError:
            if self.required:
                raise
            else:
                return ()

        # Use standard RENAME_DIMS from ixmp config
        dims = rename_dims().copy()
        values = set(dims.values())
        dims.update({d: d for d in self.key.dims if d not in values})

        c.add("load_file", path, key=self.key, dims=dims, name=self.key.name)
        return (self.key,)


ExogenousDataFile(
    "pdt-cap-ref",
    (pdt_cap / "y") + "ref",
    "Reference (historical) PDT per capita",
)

ExogenousDataFile("disutility", "disutility:n-cg-t-y:per vehicle", "LDV disutility")
ExogenousDataFile("demand-scale", dims=("n", "y"))
ExogenousDataFile(
    "energy-other",
    "energy:c-n:transport other",
    "2020 demand for other transport energy",
    required=False,
)
ExogenousDataFile("fuel-emi-intensity", dims=("c", "e"))
ExogenousDataFile("freight-activity", "freight activity:n:ref")
ExogenousDataFile("freight-mode-share-ref", "freight mode share:n-t:ref")
ExogenousDataFile(("ikarus", "availability"), dims=("source", "t", "c", "y"))
ExogenousDataFile(("ikarus", "fix_cost"), dims=("source", "t", "c", "y"))
ExogenousDataFile(("ikarus", "input"), dims=("source", "t", "c", "y"))
ExogenousDataFile(("ikarus", "inv_cost"), dims=("source", "t", "c", "y"))
ExogenousDataFile(("ikarus", "technical_lifetime"), dims=("source", "t", "c", "y"))
ExogenousDataFile(("ikarus", "var_cost"), dims=("source", "t", "c", "y"))
ExogenousDataFile("input-base", "input:t-c-h:base", "Base model input efficiency")
ExogenousDataFile("ldv-activity", "ldv activity:n:exo", "LDV activity")
ExogenousDataFile("ldv-class", dims=("n", "vehicle_class"), required=False)
ExogenousDataFile("ldv-input-adj", dims=("n", "scenario"))
ExogenousDataFile(
    "ldv-input-ref",
    "fuel economy:nl-m:ldv+ref",
    "Reference fuel economy for LDVs",
    required=False,
)
ExogenousDataFile(
    "ldv-new-capacity",
    "cap_new:nl-t-yv:ldv+exo",
    "New capacity values for LDVs",
    required=False,
)
ExogenousDataFile(
    "load-factor-ldv",
    doc="Load factor (occupancy) of LDVs",
    dims=("scenario", "n", "y"),
)
ExogenousDataFile(
    "load-factor-nonldv", doc="Load factor (occupancy) of non-LDV vehicles", dims=("t",)
)
ExogenousDataFile(("ma3t", "attitude"), dims=("attitude",))
ExogenousDataFile(
    ("ma3t", "driver"), dims=("census_division", "area_type", "driver_type")
)
ExogenousDataFile(("ma3t", "population"), dims=("census_division", "area_type"))
ExogenousDataFile("mer-to-ppp", dims=("n", "y"), required=False)
ExogenousDataFile("pdt-elasticity", dims=("scenario", "n"))
ExogenousDataFile("population-suburb-share", dims=("n", "y"), required=False)
ExogenousDataFile("speed", dims=("scenario", "n", "t", "y"))
