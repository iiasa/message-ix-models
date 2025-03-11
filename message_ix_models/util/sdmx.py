"""Utilities for handling objects from :mod:`sdmx`."""

import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum, Flag
from functools import cache
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union, cast
from warnings import warn

import sdmx
import sdmx.message
import sdmx.urn
from genno import Key
from iam_units import registry
from sdmx.model import common, v21

from .common import package_data_path
from .ixmp import get_reversed_rename_dims, rename_dims

if TYPE_CHECKING:
    from os import PathLike
    from typing import TypeVar

    import pint
    from genno import Computer, Key
    from sdmx.message import StructureMessage
    from sdmx.model.common import ConceptScheme

    from message_ix_models.types import KeyLike, MaintainableArtefactArgs

    from .context import Context

    # TODO Use "from typing import Self" once Python 3.11 is the minimum supported
    Self = TypeVar("Self", bound="AnnotationsMixIn")

log = logging.getLogger(__name__)

CodeLike = Union[str, common.Code]


#: Collection of Dataflow.
DATAFLOW: dict[str, "Dataflow"] = {}


@dataclass
class AnnotationsMixIn:
    """Mix-in for dataclasses to allow (de)serializing as SDMX annotations."""

    # TODO Type with overrides: list → list
    def get_annotations(self, _rtype: Union[type[list], type[dict]]):
        """Return a collection of :class:`.Annotation` for the fields of the object.

        Returns
        -------
        list of :class:`Annotation <sdmx.model.common.Annotation>`
            if `_rtype` is :class:`list`.
        dict
            if `_rtype` is :class:`dict`. The dict has the one key "annotations", mapped
            to a :class:`list` of Annotations. This can be used as a keyword argument
            to the constructor of a :class:`.AnnotableArtefact` subclass.
        """
        result = []
        for f in fields(self):
            anno_id = f.name.replace("_", "-")
            result.append(
                common.Annotation(id=anno_id, text=repr(getattr(self, f.name)))
            )

        if _rtype is list:
            return result
        else:
            return dict(annotations=result)

    @classmethod
    def from_obj(cls: type["Self"], obj: common.AnnotableArtefact) -> "Self":
        """Return a new instance of `cls` given an AnnotableArtefact `obj`."""
        args = []
        for f in fields(cls):
            anno_id = f.name.replace("_", "-")
            args.append(obj.eval_annotation(id=anno_id))

        return cls(*args)


class Dataflow:
    """Information about an input or output data flow.

    If an input data flow, the data is expected in a file at `path`.

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
        Human-readable name of the data flow.
    units : str
        Units for observations in the data flow.
    key : KeyLike, optional
        Key at which the data from the file will be present in a :class:`.Computer`.
    dims : tuple of str, optional
        Dimensions of the data.
    path : str or tuple of str, optional
        Path at which an input data file is located. If not supplied, :attr:`path` is
        constructed from `key`, `dims`, and the tag "exo".
    description : str, optional
        Human-readable description of the data flow, including any notes about required
        properties or contents or methods used to handle the data.
    required : bool, optional
        If :any:`True` (the default), the input data file **must** be present for the
        build to succeed.
    replace : bool, *optional*
        If :any:`True`, replace any existing entry in :data:`DATAFLOW` that with an
        equivalent URN to the one implied by `kwargs`. Otherwise (default), raise an
        exception.
    """

    #: :class:`sdmx.Dataflow <sdmx.model.common.BaseDataflowDefinition>` describing the
    #: data flow.
    df: "sdmx.model.common.BaseDataflow"

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
        replace: bool = False,
    ):
        import pint
        from sdmx.model.common import Annotation
        from sdmx.model.v21 import DataflowDefinition, DataStructureDefinition

        # Collection of annotations for the data flow
        anno = [Annotation(id="required-for-build", text=repr(required))]

        # Handle `path` argument
        if isinstance(path, str):
            _path = Path(path)
        elif path:
            _path = Path(*path)

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
            key = Key(" ".join(_path.parts).replace("-", " "), dims or (), "exo")
        else:
            # Convert to Key object
            key = Key(key)

            if path is None:
                _path = Path(key.name.replace(" ", "-"))

        anno.append(Annotation(id="genno-key", text=str(key)))

        _path = _path.with_suffix(".csv")
        anno.append(Annotation(id="file-path", text=str(_path)))

        # Retrieve the shared concept scheme
        common = common_structures()
        cs: "ConceptScheme" = common.concept_scheme["CS_MESSAGE_TRANSPORT"]
        # Reuse its properties for maintainable artefacts
        kw: "MaintainableArtefactArgs" = dict(
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
        _rrd = get_reversed_rename_dims()
        for dim in key.dims:
            # Symbol ('n') → Dimension ID ('node') → upper case
            dim_id = _rrd.get(dim, dim).upper()
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
        self.df.urn = sdmx.urn.make(self.df)

        # Add or replace an entry in DATAFLOW
        if duplicate := list(filter(lambda x: x.key == self.key, DATAFLOW.values())):
            existing = duplicate[0]
            if replace:
                log.info(f"Replace existing entry for {existing.df.urn!r}")
                DATAFLOW[self.df.urn] = self
            else:
                raise RuntimeError(
                    f"Definition of {self} duplicates existing {existing}"
                )
        else:
            # Add to the list of FILES
            DATAFLOW[self.df.urn] = self

    # Does nothing except ensure callable(…) == True for inspection by genno
    def __call__(self): ...

    def __repr__(self) -> str:
        return f"<ExogenousDataFile {self.path} → {self.key}>"

    # Access to annotations of DFD
    @property
    def key(self) -> "Key":
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

    # For interaction with genno
    def add_tasks(
        self, c: "Computer", *args, context: "Context"
    ) -> tuple["KeyLike", ...]:
        """Prepare `c` to read data from a file like :attr:`.path`."""
        # TODO Use a package-wide utility or a callback
        from message_ix_models.model.transport.util import path_fallback

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


# FIXME Reduce complexity from 13 → ≤11
def as_codes(  # noqa: C901
    data: Union[list[str], dict[str, CodeLike]],
) -> list[common.Code]:
    """Convert `data` to a :class:`list` of :class:`.Code` objects.

    Various inputs are accepted:

    - :class:`list` of :class:`str`.
    - :class:`dict`, in which keys are :attr:`~sdmx.model.common.Code.id` and values are
      further :class:`dict` with keys matching other Code attributes.
    """
    # Assemble results as a dictionary
    result: dict[str, common.Code] = {}

    if isinstance(data, list):
        # FIXME typing ignored temporarily for PR#9
        data = dict(zip(data, data))  # type: ignore [arg-type]
    elif not isinstance(data, Mapping):
        raise TypeError(data)

    for id, info in data.items():
        # Pass through Code; convert other types to dict()
        if isinstance(info, common.Code):
            result[info.id] = info
            continue
        elif isinstance(info, str):
            _info = dict(name=info)
        elif isinstance(info, Mapping):
            _info = dict(info)
        else:
            raise TypeError(info)

        # Create a Code object
        code = common.Code(
            id=str(id),
            name=_info.pop("name", str(id).title()),
        )

        # Store the description, if any
        try:
            code.description = common.InternationalString(
                value=_info.pop("description")
            )
        except KeyError:
            pass

        # Associate with a parent
        try:
            parent_id = _info.pop("parent")
        except KeyError:
            pass  # No parent
        else:
            result[parent_id].append_child(code)

        # Associate with any children
        for id in _info.pop("child", []):
            try:
                code.append_child(result[id])
            except KeyError:
                pass  # Not parsed yet

        # Convert other dictionary (key, value) pairs to annotations
        for id, value in _info.items():
            code.annotations.append(
                common.Annotation(
                    id=id, text=value if isinstance(value, str) else repr(value)
                )
            )

        result[code.id] = code

    return list(result.values())


@cache
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


def eval_anno(obj: common.AnnotableArtefact, id: str):
    """Retrieve the annotation `id` from `obj`, run :func:`eval` on its contents.

    .. deprecated:: 2023.9.12

       Use :meth:`sdmx.model.common.AnnotableArtefact.eval_annotation`, which provides
       the same functionality.
    """
    warn(
        "message_ix_models.util.eval_anno; use sdmx.model.common.AnnotableArtefact"
        ".eval_annotation() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    try:
        value = str(obj.get_annotation(id=id).text)
    except KeyError:  # No such attribute
        return None

    try:
        return eval(value, {"registry": registry})
    except Exception as e:  # Something that can't be eval()'d, e.g. a plain string
        log.debug(f"Could not eval({value!r}): {e}")
        return value


class URNLookupEnum(Enum):
    """:class:`.Enum` subclass that allows looking up members using a URN."""

    _ignore_ = "_urn_name"
    _urn_name: dict

    def __init_subclass__(cls):
        cls._urn_name = dict()

    @classmethod
    def by_urn(cls, urn: str):
        """Return the :class:`.Enum` member given its `urn`."""
        return cls[cls.__dict__["_urn_name"][urn]]


def get_cl(name: str, context: Optional["Context"] = None) -> "common.Codelist":
    """Return a code list."""
    from message_ix_models.model.structure import get_codes

    id_ = None
    if name == "NODE" and context:
        name, id_ = f"node/{context.model.regions}", f"NODE_{context.model.regions}"
    elif name == "YEAR" and context:
        name, id_ = f"year/{context.model.years}", f"YEAR_{context.model.years}"

    name = name or name.lower()
    id_ = id_ or name.upper()

    as_ = read("IIASA_ECE:AGENCIES")
    cl: "common.Codelist" = common.Codelist(
        id=f"CL_{id_}",
        name=f"Codes for message-ix-models concept {name!r}",
        maintainer=as_["IIASA_ECE"],
        # FIXME remove str() once sdmx1 > 2.21.1 can handle Version
        version=str(get_version()),
        is_external_reference=False,
        is_final=True,
    )
    cl.urn = sdmx.urn.make(cl)

    try:
        cl.extend(get_codes(name.lower()))
    except FileNotFoundError:
        pass

    return cl


@cache
def get_cs() -> "common.ConceptScheme":
    """Return a scheme of common concepts for the MESSAGEix-GLOBIOM model family.

    The full artefact contains its own detailed description.
    """
    from .ixmp import rename_dims

    cs = common.ConceptScheme(
        id="CS_MESSAGE_IX_MODELS",
        name="Concepts for message-ix-models",
        description="""These include:

1. Concepts used as dimensions in MESSAGE parameter data (see also :mod:`.structure`).
2. Concepts particular to variants of MESSAGEix-GLOBIOM, such as
   :mod:`.model.transport`.

Each concept in the concept scheme has:

- An upper case :py:`.id`, for instance :py:`"TECHNOLOGY"`.
- An annotation with :py:`id="aliases"` which is the :func:`repr` of a :class:`set`
  giving alternate labels understood to be equivalent. These include
  :data:`ixmp.report.RENAME_DIMS`, for example :py:`"t"` for 'technology'.""",
        maintainer=common.Agency(id="IIASA_ECE"),
        version="1.0.0",
    )

    # Add concepts for MESSAGE sets/dimensions
    for k, v in rename_dims().items():
        # Retrieve or create the Concept for the set (e.g. "year" for k="year_act")
        set_name = k.split("_")[0]
        concept = cs.setdefault(
            id=set_name.upper(),
            name=f"{set_name!r} MESSAGEix set",
            annotations=[common.Annotation(id="aliases", text=repr(set()))],
        )
        # Add `v` to the aliases annotation
        anno = concept.get_annotation(id="aliases")
        anno.text = repr(eval(str(anno.text)) | {v})

    for c_id in "MODEL", "SCENARIO", "VERSION":
        cs.setdefault(
            id=c_id,
            name=f"{c_id.lower()!r} ixmp scenario identifier",
            description="""In the ixmp data model, scenario objects are identified by
unique keys including (model name, scenario name, version).""",
        )

    cs.setdefault(
        id="UNIT_MEASURE",
        name="Unit of measure",
        description="Unit in which data values are expressed",
        annotations=[
            common.Annotation(
                id="same-as-urn",
                text="urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).UNIT_MEASURE",
            ),
        ],
    )
    cs.setdefault(
        id="URL",
        name="ixmp scenario URL",
        description="""URL combining the platform name (~database), model name, scenario
name, and version of an ixmp Scenario. See
https://docs.messageix.org/projects/ixmp/en/stable/api.html#ixmp.TimeSeries.url""",
    )

    return cs


@cache
def get_concept(string: str) -> "common.Concept":
    """Retrieve a single Concept from :func:`get_cs`."""
    for concept in get_cs().items.values():
        labels = [concept.id] + list(concept.eval_annotation(id="aliases") or [])
        if re.fullmatch("|".join(labels), string, flags=re.IGNORECASE):
            return concept
    raise ValueError(string)


def get_version() -> "common.Version":
    """Return a :class:`sdmx.model.common.Version` for :mod:`message_ix_models`."""
    return common.Version(version(__package__.split(".")[0]).split("+")[0])


def make_dataflow(
    id: str,
    dims: Sequence[str],
    name: Optional[str] = None,
    ma_kwargs: Optional["MaintainableArtefactArgs"] = None,
    context: Optional["Context"] = None,
    message: Optional["sdmx.message.StructureMessage"] = None,
) -> "sdmx.message.StructureMessage":
    """Create and store an SDMX 2.1 DataflowDefinition (DFD) and related structures.

    Parameters
    ----------
    id :
        Partial ID of both the DFD and a related DataStructureDefinition (DSD).
    dims :
        IDs of the dimensions of the DSD. These may be short dimension IDs as used in
        :mod:`message_ix.report`, for instance :py:`"t"` for the 'technology' dimension.
    ma_kwargs :
        Common keyword arguments for all SDMX MaintainableArtefacts created.

    Returns
    -------
    sdmx.message.StructureMessage
        …containing:

        - 1 :class:`.DataflowDefinition`.
        - 1 :class:`.DataStructureDefinition`.
        - 1 :class:`.ConceptScheme`, ``IIASA_ECE:CS_COMMON``.
        - For each dimension indicated by `dims`, a :class:`Codelist`.
    """
    from sdmx import urn

    sm = message or sdmx.message.StructureMessage()

    if ma_kwargs is None:
        ma_kwargs = {}
    ma_kwargs.setdefault("maintainer", common.Agency(id="IIASA_ECE"))
    ma_kwargs.setdefault("is_external_reference", False)
    ma_kwargs.setdefault("is_final", True)
    # FIXME remove str() once sdmx1 > 2.21.1 can handle Version
    ma_kwargs.setdefault("version", str(get_version()))

    # Create the data structure definition
    dsd = v21.DataStructureDefinition(id=f"DS_{id.upper()}", **ma_kwargs)
    dsd.measures.getdefault(id="value")
    sm.add(dsd)

    # Create the data flow definition
    dfd = v21.DataflowDefinition(id=f"DF_{id.upper()}", **ma_kwargs, structure=dsd)
    dfd.urn = urn.make(dfd)
    if name:
        dfd.description = name
    sm.add(dfd)

    # Add the common concept scheme
    sm.add(get_cs())

    # Add dimensions to the DSD according to `dims`
    for order, dim_id in enumerate(dims):
        # Retrieve the dimension concept and its full ID
        concept = get_concept(dim_id)

        # Create a code list for this dimension
        cl = get_cl(concept.id, context=context)
        sm.add(cl)

        # Create the dimension
        dsd.dimensions.getdefault(
            id=dim_id,
            concept_identity=concept,
            local_representation=common.Representation(enumerated=cl),
            order=order,
        )

    # Add attributes
    nsr = v21.NoSpecifiedRelationship()
    for attr_id in "MODEL", "SCENARIO", "VERSION", "UNIT_MEASURE":
        # Retrieve the attribute concept and its full ID
        concept = get_concept(attr_id)

        dsd.attributes.getdefault(id=attr_id, concept_identity=concept, related_to=nsr)

    return sm


def make_enum(urn, base=URNLookupEnum):
    """Create an :class:`.enum.Enum` (or `base`) with members from codelist `urn`."""
    # Read the code list
    cl = read(urn)

    # Ensure the 0 member is NONE, not any of the codes
    names = ["NONE"] if issubclass(base, Flag) else []
    names.extend(code.id for code in cl)

    # Create the class
    result = base(urn, names)

    if issubclass(base, URNLookupEnum):
        # Populate the URN → member name mapping
        for code in cl:
            result._urn_name[code.urn] = code.id

    return result


def read(urn: str, base_dir: Optional["PathLike"] = None):
    """Read SDMX object from package data given its `urn`."""
    # Identify a path that matches `urn`
    base_dir = Path(base_dir or package_data_path("sdmx"))
    urn = urn.replace(":", "_")  # ":" invalid on Windows
    paths = sorted(
        set(base_dir.glob(f"*{urn}*.xml")) | set(base_dir.glob(f"*{urn.upper()}*.xml"))
    )

    if len(paths) > 1:
        log.info(
            f"Match {paths[0].relative_to(base_dir)} for {urn!r}; {len(paths) - 1} "
            "other result(s)"
        )

    try:
        with open(paths[0], "rb") as f:
            msg = cast("StructureMessage", sdmx.read_sdmx(f))
    except IndexError:
        raise FileNotFoundError(f"'*{urn}*.xml', '*{urn.upper()}*.xml' or similar")

    for _, cls in msg.iter_collections():
        try:
            return next(iter(msg.objects(cls).values()))
        except StopIteration:
            pass


def write(obj, base_dir: Optional["PathLike"] = None, basename: Optional[str] = None):
    """Store an SDMX object as package data."""
    base_dir = Path(base_dir or package_data_path("sdmx"))

    if isinstance(obj, sdmx.message.StructureMessage):
        msg = obj
        assert basename
    else:
        # Set the URN of the object
        obj.urn = sdmx.urn.make(obj)

        # Wrap the object in a StructureMessage
        msg = sdmx.message.StructureMessage()
        msg.add(obj)

        # Identify a path to write the file. ":" is invalid on Windows.
        basename = basename or obj.urn.split("=")[-1].replace(":", "_")

    msg.header = sdmx.message.Header(
        source=f"Generated by message_ix_models {version('message_ix_models')}",
        prepared=datetime.now(),
    )

    path = base_dir.joinpath(f"{basename}.xml")

    # Write
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(sdmx.to_xml(msg, pretty_print=True))

    log.info(f"Wrote {path}")


def register_agency(agency: "common.Agency") -> "common.AgencyScheme":
    """Add `agency` to the :class:`.AgencyScheme` "IIASA_ECE:AGENCIES"."""
    # Read the existing agency scheme
    as_ = read("IIASA_ECE:AGENCIES")

    if agency in as_:
        log.info(f"Replace or update existing {as_[agency.id]!r}")
        as_.items[agency.id] = agency
    else:
        as_.append(agency)

    log.info(f"Updated {as_!r}")

    # Write to file again
    write(as_)

    return as_
