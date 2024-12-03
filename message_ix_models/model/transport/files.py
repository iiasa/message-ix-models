import logging
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

from genno import Key

from message_ix_models.util import package_data_path
from message_ix_models.util.ixmp import get_reversed_rename_dims

from .key import pdt_cap

if TYPE_CHECKING:
    import genno
    import pint
    import sdmx.message
    import sdmx.model.common
    from genno.core.key import KeyLike
    from sdmx.model.common import ConceptScheme

    from message_ix_models import Context

log = logging.getLogger(__name__)


#: List of all :class:`.ExogenousDataFile`.
FILES: list["ExogenousDataFile"] = []


class ExogenousDataFile:
    """Exogenous/input data for MESSAGEix-Transport expected in a file.

    .. todo::
       - Generate documentation (docstrings or snippets) in reStructuredText format.
       - Accept an argument that sets :attr:`dfd` directly; skip handling other
         arguments.
       - Annotate certain dimensions as optional; expand :meth:`.add_tasks` to
         automatically handle insertion of these dimensions.
       - Merge with, or make a subclass of, :class:`.ExoData`.

    Parameters
    ----------
    name : str
        Name of the data flow.
    units : str
        Units for observations in the data flow.
    key : KeyLike, optional
        Key at which the data from the file will be present in a :class:`.Computer`.
    dims : tuple of str, optional
        Dimensions of the data.
    path : str or tuple of str, optional
        Path at which the file is located. If not supplied, :attr:`path` is constructed
        from `key`, `dims`, and the tag "exo".
    description : str, optional
        Human-readable description of the data flow, including any notes about required
        properties or contents or methods used to handle the data.
    required : bool, optional
        If :any:`True` (the default), the file **must** be present for the build to
        succeed.
    """

    #: :class:`sdmx.Dataflow <sdmx.model.common.BaseDataflowDefinition>` describing the
    #: input data flow.
    df: "sdmx.model.common.BaseDataflowDefinition"

    # Access to annotations of DFD
    @property
    def key(self) -> Key:
        """:class:`genno.Key`, including preferred dimensions."""
        return Key(str(self.df.get_annotation(id="genno-key").text))

    @property
    def path(self) -> Path:
        """Path fragment for the location of a file containing the data."""
        return Path(str(self.df.get_annotation(id="file-path").text))

    @property
    def required(self) -> bool:
        """:any:`True` if the data must be present for :func:`.transport.build.main`."""
        return self.df.eval_annotation(id="required-for-build")

    @property
    def units(self) -> "pint.Unit":
        """Preferred units."""
        import pint

        return pint.get_application_registry().Unit(
            self.df.eval_annotation(id="preferred-units")
        )

    def __init__(
        self,
        *,
        name: str,
        units: str,
        key: Optional["KeyLike"] = None,
        dims: Optional[tuple[str, ...]] = None,
        path: Union[str, tuple[str, ...], None] = None,
        description: Optional[str] = None,
        required: bool = True,
    ):
        import pint
        from sdmx.model.common import Annotation
        from sdmx.model.v21 import DataflowDefinition, DataStructureDefinition

        # Collection of annotations for the data flow
        anno = [Annotation(id="required-for-build", text=repr(required))]

        # Handle `path` argument
        if isinstance(path, str):
            path = Path(path)
        elif path:
            path = Path(*path)

        # Parse and store units
        ureg = pint.get_application_registry()
        try:
            units = ureg.Unit(units)
        except Exception as e:
            log.info(f"Replace units {units!r} with 'dimensionless' due to {e}")
            units = ureg.dimensionless
        anno.append(Annotation(id="preferred-units", text=f"{units}"))

        if not key:
            # Determine from file path
            key = Key(" ".join(path.parts).replace("-", " "), dims or (), "exo")
        else:
            # Convert to Key object
            key = Key(key)

            if path is None:
                path = Path(key.name.replace(" ", "-"))

        anno.append(Annotation(id="genno-key", text=str(key)))

        path = path.with_suffix(".csv")
        anno.append(Annotation(id="file-path", text=str(path)))

        # Retrieve the shared concept scheme
        common = common_structures()
        cs: "ConceptScheme" = common.concept_scheme["CS_MESSAGE_TRANSPORT"]
        # Reuse its properties for maintainable artefacts
        kw = dict(
            maintainer=cs.maintainer,
            version=cs.version,
            is_final=cs.is_final,
            is_external_reference=cs.is_external_reference,
        )

        # SDMX IDs for the data flow and data structure
        name_for_id = key.name.upper().replace(" ", "_")
        df_id = f"DF_{name_for_id}"
        ds_id = f"DS_{name_for_id}"

        # Create a data structure definition
        dsd = DataStructureDefinition(id=ds_id, **kw, name=f"Structure of {df_id}")

        # Add dimensions
        dims = get_reversed_rename_dims()
        for dim in key.dims:
            # Symbol ('n') → Dimension ID ('node') → upper case
            dim_id = dims.get(dim, dim).upper()
            # Add to the concept scheme
            concept = cs.setdefault(id=dim_id)
            # Add the dimension to the DSD
            dsd.dimensions.getdefault(id=dim_id, concept_identity=concept)

        if description is not None:
            desc = f"{description.strip()}\n\n"
        else:
            desc = ""
        desc += "Input data for MESSAGEix-Transport."

        # Create and store a data flow definition
        self.df = DataflowDefinition(
            id=df_id, **kw, name=name, description=desc, structure=dsd, annotations=anno
        )

    # Does nothing except ensure callable(…) == True for inspection by genno
    def __call__(self): ...

    def __repr__(self) -> str:
        return f"<ExogenousDataFile {self.path} → {self.key}>"

    def add_tasks(
        self, c: "genno.Computer", *args, context: "Context"
    ) -> tuple["KeyLike", ...]:
        """Prepare `c` to read data from a file like :attr:`.path`."""
        from message_ix_models.util.ixmp import rename_dims

        from .util import path_fallback

        # Identify the path
        try:
            path = path_fallback(context, self.path)
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

    def generate_csv_template(self) -> Path:
        """Generate a CSV template file."""
        raise NotImplementedError
        # 1. In the current format.abs
        # 2. In SDMX-CSV.
        # dm = DataMessage()
        # dm.data.append(DataSet(structure))
        # template =


def add(*, replace: bool = False, **kwargs):
    """Add or replace an entry in :data:`FILES`.

    Parameters
    ----------
    replace : bool, *optional*
        If :any:`True`, replace any existing entry in :data:`FILES` that targets an
        equivalent key to the one implied by `kwargs`. Otherwise (default), raise an
        exception.
    kwargs :
        Passed on to :class:`ExogenousDataFile`.
    """
    edf = ExogenousDataFile(**kwargs)

    if duplicate := list(filter(lambda x: x[1].key == edf.key, enumerate(FILES))):
        i, existing = duplicate[0]
        if replace:
            log.info(f"Replace existing entry for {existing.key} at index {i}")
            FILES[i] = edf
        else:
            raise RuntimeError(f"Definition of {edf} duplicates existing {existing}")
    else:
        # Add to the list of FILES
        FILES.append(edf)

    return edf.key


@lru_cache()
def common_structures() -> "sdmx.message.StructureMessage":
    """Return common structures for use in the current module."""
    from importlib.metadata import version

    from packaging.version import parse
    from sdmx.message import StructureMessage
    from sdmx.model.common import ConceptScheme

    from message_ix_models.util.sdmx import read

    # Create a shared concept scheme with…
    # - Same maintainer "IIASA_ECE" as in "IIASA_ECE:AGENCIES".
    # - Version based on the current version of message_ix_models.
    # - Final and not an external reference
    cs = ConceptScheme(
        id="CS_MESSAGE_TRANSPORT",
        maintainer=read("IIASA_ECE:AGENCIES")["IIASA_ECE"],
        version=parse(version("message_ix_models")).base_version,
        is_final=False,
        is_external_reference=False,
    )

    # Return encapsulated in a StructureMessage
    sm = StructureMessage()
    sm.add(cs)
    return sm


def collect_structures() -> "sdmx.message.StructureMessage":
    """Collect all SDMX data structures from :data:`FILES` and store.

    The structural metadata are written to :file:`transport-in.xml`.
    """
    from message_ix_models.util.sdmx import write

    sm = common_structures()

    for file in FILES:
        # Add both the DFD and DSD
        sm.add(file.df)
        sm.add(file.df.structure)

    write(sm, basename="transport-in")

    return sm


def read_structures() -> "sdmx.message.StructureMessage":
    """Read structural metadata from :file:`transport-in.xml`."""
    import sdmx

    with open(package_data_path("sdmx", "transport-in.xml"), "rb") as f:
        return sdmx.read_sdmx(f)


activity_freight = add(
    key="freight activity:n:exo",
    name="Freight transport activity",
    units="Gt / km",
)

activity_ldv = add(
    key="ldv activity:n:exo",
    name="Activity (driving distance) per light duty vehicle",
    units="km / year",
)

pdt_cap_proj = add(
    key="P activity:scenario-n-t-y:exo",
    path="pdt-cap",
    name="Projected passenger-distance travelled (PDT) per capita",
    units="km / passenger / year",
    required=False,
)

pdt_cap_ref = add(
    key=(pdt_cap / "y") + "ref",
    path="pdt-cap-ref",
    name="Reference (historical) passenger-distance travelled (PDT) per capita",
    units="km / year",
)

disutility = add(
    key="disutility:n-cg-t-y:per vehicle",
    name="Disutility cost of LDV usage",
    units="kUSD / vehicle",
)

demand_scale = add(
    key="demand-scale:n-y",
    name="Scaling of total demand relative to base year levels",
    units="dimensionless",
)

energy_other = add(
    key="energy:c-n:transport other",
    path="energy-other",
    name="2020 demand for other transport energy",
    units="TJ",
    required=False,
)

fuel_emi_intensity = add(
    key="fuel-emi-intensity:c-e",
    name="Carbon emissions intensity of fuel use",
    description="""Values are in GWP-equivalent mass of carbon, not in mass of the
emissions species.""",
    units="tonne / kWa",
)

lifetime_ldv = add(
    key="lifetime:nl-yv:ldv+exo",
    path="lifetime-ldv",
    name="Technical lifetime (maximum age) of LDVs",
    description="""Values are filled forwards. In MESSAGE(V)-Transport, this quantity
had the additional dimension of driver_type, and values were 20 years for
driver_type='average', 15 y for 'moderate', and 10 y for 'frequent'.""",
    units="year",
)

mode_share_freight = add(
    key="freight mode share:n-t:exo",
    path="freight-mode-share-ref",
    name="Mode shares of freight activity in the model base period",
    units="dimensionless",
)

ikarus_availability = add(
    path=("ikarus", "availability"),
    dims=("source", "t", "c", "y"),
    name="Availability of non-LDV transport technologies",
    description="""- 'source' is either "IKARUS" or "Krey/Linßen".""",
    units="km / a",
)

ikarus_fix_cost = add(
    path=("ikarus", "fix_cost"),
    dims=("source", "t", "c", "y"),
    name="Fixed cost of non-LDV transport technologies",
    description="Costs are per vehicle.",
    units="kEUR_2000",
)

ikarus_input = add(
    path=("ikarus", "input"),
    dims=("source", "t", "c", "y"),
    name="Input energy intensity of non-LDV transport technologies",
    units="GJ / (100 vehicle km)",
)

ikarus_inv_cost = add(
    path=("ikarus", "inv_cost"),
    dims=("source", "t", "c", "y"),
    name="Investment/capital cost of non-LDV transport technologies",
    units="MEUR_2000",
)

ikarus_technical_lifetime = add(
    path=("ikarus", "technical_lifetime"),
    dims=("source", "t", "c", "y"),
    name="Technical lifetime of non-LDV transport technologies",
    units="year",
)

ikarus_var_cost = add(
    path=("ikarus", "var_cost"),
    dims=("source", "t", "c", "y"),
    name="Variable cost of non-LDV transport technologies",
    units="EUR_2000 / (100 vehicle km)",
)

input_base = add(
    path="input-base",
    key="input:t-c-h:base",
    name="Base model input efficiency",
    units="GWa",
)

age_ldv = add(
    path="ldv-age",
    key="age:n-t-y:ldv+exo",
    name="Mean age of LDVs as of the model base period",
    units="years",
)

class_ldv = add(
    path="ldv-class",
    dims=("n", "vehicle_class"),
    name="Share of light-duty vehicles by class",
    units="dimensionless",
    required=False,
)

input_adj_ldv = add(
    key="ldv input adj:n-scenario:exo",
    name="Calibration factor for LDV fuel economy",
    units="dimensionless",
)

input_ref_ldv = add(
    path="ldv-input-ref",
    key="fuel economy:nl-m:ldv+ref",
    name="Reference fuel economy for LDVs",
    units="GWa / (Gvehicle km)",
    required=False,
)

cap_new_ldv = add(
    path="ldv-new-capacity",
    key="cap_new:nl-t-yv:ldv+exo",
    name="New capacity values for LDVs",
    description="""Applied as historical_new_capacity and bound_new_capacity_{lo,up}
values for LDVs""",
    units="MVehicle",
    required=False,
)

t_share_ldv = add(
    path="ldv-t-share",
    key="tech share:n-t:ldv+exo",
    name="Share of total stock for LDV technologies",
    description="""
- Values must sum to 1 across the 't' dimension.
- Technology codes annotated "historical-only: True" (e.g. ICE_L_ptrp) must be omitted
  or have zero values. If not, incompatible/infeasible constraint values are created.
""",
    units="dimensionless",
)

act_non_ldv = add(
    path="act-non_ldv.csv",
    key="activity:n-t-y:non-ldv+exo",
    name="Fixed activity of non-LDV technologies.",
    units="dimensionless",
)

load_factor_ldv = add(
    key="load factor ldv:scenario-n-y:exo",
    name="Load factor (occupancy) of LDVs",
    description="""Units are implicity passengers per vehicle.""",
    units="dimensionless",
)

load_factor_nonldv = add(
    key="load factor nonldv:t:exo",
    name="Load factor (occupancy) of non-LDV passenger vehicles",
    units="passenger / vehicle",
)

pop_share_attitude = add(
    path=("ma3t", "attitude"),
    dims=("attitude",),
    name="Share of population by technology propensity/attitude",
    units="dimensionless",
)

pop_share_driver = add(
    path=("ma3t", "driver"),
    dims=("census_division", "area_type", "driver_type"),
    name="Share of population by driver type, census_division, and area_type",
    description="""Values sum to roughly 1 across 'area_type' for each combination of
other dimensions.""",
    units="dimensionless",
)

pop_share_cd_at = add(
    path=("ma3t", "population"),
    dims=("census_division", "area_type"),
    name="Share of population by census division and area type",
    description="Values sum to roughly 1 across 'area_type' for each census_division.",
    units="dimensionless",
)

mer_to_ppp = add(
    key="mer to ppp:n-y",
    name="Convertion from market exchange rate (MER) to PPP",
    units="dimensionless",
    required=False,
)

pdt_elasticity = add(
    key="pdt elasticity:scenario-n:exo",
    name="“Elasticity” of PDT-capita with respect to GDP",
    units="dimensionless",
)

population_suburb_share = add(
    key="population suburb share:n-y:exo",
    name="Share of MSA population that is suburban",
    units="dimensionless",
    required=False,
)

speed = add(
    key="speed:scenario-n-t-y:exo",
    name="Vehicle speed",
    description="""
- This is the mean value for all vehicles of all technologies for the given mode.
- The code that handles this file interpolates on the ‘year’ dimension.""",
    units="km / hour",
)
