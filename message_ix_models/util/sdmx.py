"""Utilities for handling objects from :mod:`sdmx`."""

import logging
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum, Flag, auto

# TODO Remove when Python 3.10 is no longer supported
from enum import EnumMeta as EnumType
from functools import cache
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Generic, Optional, TypeVar, Union, cast
from warnings import warn

import sdmx
import sdmx.message
import sdmx.urn
from genno import Key
from iam_units import registry
from ixmp.report.common import RENAME_DIMS
from sdmx.model import common, v21

from .common import package_data_path
from .context import Context

if TYPE_CHECKING:
    from os import PathLike

    import pint
    from genno import Computer, Key
    from sdmx.message import StructureMessage

    from message_ix_models.types import KeyLike

    # TODO Use "from typing import Self" once Python 3.11 is the minimum supported
    Self = TypeVar("Self", bound="AnnotationsMixIn")


log = logging.getLogger(__name__)

CodeLike = Union[str, common.Code]

#: Collection of :class:`.Dataflow` instances.
DATAFLOW: dict[str, "Dataflow"] = {}

#: Common store of SDMX structural artefacts.
STORE = sdmx.message.StructureMessage()


@dataclass
class AnnotationsMixIn:
    """Mix-in for dataclasses to allow (de)serializing as SDMX annotations."""

    # TODO Type with overrides: list → list
    def get_annotations(self, _rtype: Union[type[list], type[dict]]):
        """Return a collection of :class:`.Annotation` for the fields of the object.

        Returns
        -------
        list of :class:`Annotation <sdmx.model.common.BaseAnnotation>`
            if `_rtype` is :class:`list`.
        dict
            if `_rtype` is :class:`dict`. The dict has the one key "annotations", mapped
            to a :class:`list` of Annotations. This can be used as a keyword argument
            to the constructor of a :class:`.AnnotableArtefact` subclass.
        """
        result = []
        for f in fields(self):
            anno_id = f.name.replace("_", "-")
            result.append(v21.Annotation(id=anno_id, text=repr(getattr(self, f.name))))

        if _rtype is list:
            return result
        else:
            return dict(annotations=result)

    @classmethod
    def from_obj(
        cls: type["Self"], obj: common.AnnotableArtefact, globals: Optional[dict] = None
    ) -> "Self":
        """Return a new instance of `cls` given an AnnotableArtefact `obj`."""
        args = []
        for f in fields(cls):
            anno_id = f.name.replace("_", "-")
            args.append(obj.eval_annotation(id=anno_id, globals=globals))

        return cls(*args)


class Dataflow:
    """Information about an input or output data flow.

    If an input data flow, the data is expected in a file at `path`.

    .. todo::
       - Accept an argument that sets :attr:`df` directly and ignores other arguments.
       - Annotate certain dimensions as optional; expand :meth:`.add_tasks` to
         automatically handle insertion of these dimensions.
       - Merge with :class:`.ExoDataSource`.

    Parameters
    ----------
    module : str
        Should be the name of the code module responsible for creating the data flow,
        for instance using :py:`__name__`.
    id : str
        Partial ID of both the DFD and a related DataStructureDefinition. The strings
        :py:`"DF_"` and :py:`"DS_"` are automatically prepended, and the characters " "
        and "-" replaced with underscores.
    name : str
        Human-readable name of the data flow.
    units : str
        Units for observations in the data flow.
    key : KeyLike, optional
        Key at which the data from the file will be present in a :class:`.Computer`.
    dims : tuple of str, optional
        IDs of dimensions of the DSD. These may be short dimension IDs as used in
        :mod:`message_ix.report`, for instance :py:`"t"` for the 'technology' dimension.
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

    class FLAG(Flag):
        """Flags for :attr:`.intent`."""

        #: Input data flow.
        IN = 1
        #: Output data flow.
        OUT = 2

    #: :class:`sdmx.Dataflow <sdmx.model.common.BaseDataflowDefinition>` describing the
    #: data flow.
    #:
    #: This instance will always have annotations with the following IDs:
    #:
    #: - "file-path": see :attr:`.path`.
    #: - "genno-key": see :attr:`.key`.
    #: - "intent": see :attr:`.intent`.
    #: - "preferred-units": see :attr:`.units`.
    #: - "required-for-build": see :attr:`.required`.
    df: "sdmx.model.common.BaseDataflow"

    def __init__(
        self,
        *,
        module: str,
        id: Optional[str] = None,
        name: str,
        units: str,
        description: Optional[str] = None,
        key: Optional["KeyLike"] = None,
        dims: Optional[tuple[str, ...]] = None,
        i_o: FLAG = FLAG.IN,
        path: Union[str, tuple[str, ...], None] = None,
        required: bool = True,
        # Used only for creation
        replace: bool = False,
        cs_urn: list[str] = [],
        # Others
        **ma_kwargs,
    ):
        import pint

        # Ensure CS_MESSAGE_IX_MODELS is available
        get_cs()

        # Collection of annotations for the data flow
        anno = [
            v21.Annotation(id="intent", text=str(i_o.name)),
            v21.Annotation(id="module", text=module),
            v21.Annotation(
                id="required-for-build", text=repr((i_o & self.FLAG.IN) and required)
            ),
        ]

        # Handle `path` argument
        if isinstance(path, str):
            _path = Path(path)  # Convert str to Path
        elif path:
            _path = Path(*path)  # Something else, not None

        # Parse and store units
        ureg = pint.get_application_registry()
        try:
            units = ureg.Unit(units)
        except Exception as e:
            log.info(f"Replace units {units!r} with 'dimensionless' due to {e}")
            units = ureg.dimensionless
        anno.append(v21.Annotation(id="preferred-units", text=f"{units}"))

        if not key:
            # Determine from file path
            key = Key(" ".join(_path.parts).replace("-", " "), dims or (), "exo")
        else:
            # Convert to Key object
            key = Key(key)

            if path is None:
                _path = Path(key.name.replace(" ", "-"))

        anno.append(v21.Annotation(id="genno-key", text=str(key)))

        _path = _path.with_suffix(".csv")
        anno.append(v21.Annotation(id="file-path", text=str(_path)))

        # Default properties for maintainable artefacts
        ma_kwargs.setdefault("maintainer", read("IIASA_ECE:AGENCIES")["IIASA_ECE"])
        ma_kwargs.setdefault("is_external_reference", False)
        ma_kwargs.setdefault("is_final", False)
        ma_kwargs.setdefault("version", get_version())

        # IDs for the data flow and data structure
        name_for_id = (id or key.name).upper().replace(" ", "_").replace("-", "_")
        df_id = f"DF_{name_for_id}"
        ds_id = f"DS_{name_for_id}"

        # Create a data structure definition
        ma_kwargs["name"] = f"Structure of {df_id}"
        dsd = v21.DataStructureDefinition(id=ds_id, **ma_kwargs)
        dsd.urn = sdmx.urn.make(dsd)

        # Add dimensions to `DSD` according to `key.dims`
        for order, dim_id in enumerate(key.dims):
            # Retrieve the dimension concept and its full ID
            concept = get_concept(dim_id, cs_urn=tuple(cs_urn))

            # Create a code list for this dimension
            cl = get_cl(concept.id)

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

            dsd.attributes.getdefault(
                id=attr_id, concept_identity=concept, related_to=nsr
            )

        # Add a measure
        dsd.measures.getdefault(id="value")

        # Create and store a data flow definition
        ma_kwargs["name"] = name
        ma_kwargs["description"] = (
            f"{description.strip()}\n\n" if description is not None else ""
        )
        ma_kwargs["annotations"] = anno
        self.df = v21.DataflowDefinition(id=df_id, **ma_kwargs, structure=dsd)
        self.df.urn = sdmx.urn.make(self.df)

        # Update the instance with a docstring
        self._update_doc()

        # Add or replace an entry in DATAFLOW
        if duplicate := list(
            filter(
                lambda x: x.key == self.key and x.intent == self.intent,
                DATAFLOW.values(),
            )
        ):
            existing = duplicate[0]
            if replace:
                log.info(f"Replace existing entry for {existing.df.urn!r}")
                DATAFLOW[self.df.urn] = self
            else:
                raise RuntimeError(
                    f"Definition of {self} duplicates existing {existing}"
                )
        else:
            # Add to the collection of DATAFLOW and the STORE
            DATAFLOW[self.df.urn] = self
            STORE.add(self.df)
            STORE.add(dsd)

    # Does nothing except ensure callable(…) == True for inspection by genno
    def __call__(self): ...

    def __repr__(self) -> str:
        assert self.df.urn is not None
        return f"<Dataflow wrapping {sdmx.urn.shorten(self.df.urn)!r}>"

    def _update_doc(self) -> None:
        """Update the instance with a docstring."""
        lines = [
            f"{self.df.name} [{self.units}]",
            "",
            f"- Key and dimensions: **<{self.key}>**",
        ]
        if self.intent & self.FLAG.IN:
            # Lines only relevant for input quantities
            lines.append(f"- Input file at path: :file:`{self.path}`")
            lines.append(f"- Required for build: {self.required}")
        else:
            lines.append(
                f"- Output data from :mod:`{self.df.eval_annotation('module')}`"
            )
        lines.append(f"\n{self.df.description}")

        self.__doc__ = "\n".join(lines)

    # Access to annotations of DFD
    @property
    def intent(self) -> FLAG:
        """Indicates whether the dataflow is for input or output."""
        return self.FLAG[str(self.df.get_annotation(id="intent").text)]

    @property
    def key(self) -> "Key":
        """:class:`genno.Key`, including preferred dimensions.

        When :attr:`intent` is :data:`FLAG.IN`, this is the key at which the loaded data
        is available. When :attr:`intent` is :data:`FLAG.OUT`, this is the key from
        which the output data will be prepared.
        """
        return Key(str(self.df.get_annotation(id="genno-key").text))

    @property
    def module(self) -> str:
        """Module with which the data flow is associated."""
        return str(self.df.get_annotation(id="module").text)

    @property
    def path(self) -> Path:
        """Path fragment for the location of a file containing input data."""
        return Path(str(self.df.get_annotation(id="file-path").text))

    @property
    def required(self) -> bool:
        """:any:`True` if the input file :path:`.path` must be present to build a model.

        This means that a corresponding function, for instance
        :func:`.transport.build.main`, expects the file to be present.
        """
        return self.df.eval_annotation(id="required-for-build")

    @property
    def units(self) -> "pint.Unit":
        """Preferred units for the data."""
        import pint

        return pint.get_application_registry().Unit(
            str(self.df.get_annotation(id="preferred-units").text)
        )

    # For interaction with genno
    def add_tasks(
        self, c: "Computer", *args, context: "Context"
    ) -> tuple["KeyLike", ...]:
        """Prepare `c` to read data from a file at :attr:`.path`."""
        # TODO Use a package-wide utility or a callback
        from message_ix_models.model.transport.util import region_path_fallback

        # Identify the path
        try:
            path = region_path_fallback(context, self.path)
        except FileNotFoundError:
            if self.required:
                raise
            else:
                return ()

        # Use standard RENAME_DIMS from ixmp config
        dims = RENAME_DIMS.copy()
        values = set(dims.values())
        dims.update({d: d for d in self.key.dims if d not in values})

        c.add("load_file", path, key=self.key, dims=dims, name=self.key.name)
        return (self.key,)

    def generate_csv_template(self) -> Path:
        """Generate a CSV template file.

        Currently not implemented.
        """
        raise NotImplementedError
        # 1. In the current format.abs
        # 2. In SDMX-CSV.
        # dm = DataMessage()
        # dm.data.append(DataSet(structure))
        # template =


T = TypeVar("T", bound=Enum)


# TODO Replace with URNLookupMixin[T] once Python 3.10 is no longer supported
class URNLookupMixin(Generic[T]):
    """:class:`.Enum` mix-in class for looking up members by URN/retrieving URNs."""

    name: str
    _member_map_: dict[str, T]
    _urn_name: dict[str, str]

    @classmethod
    def by_urn(cls, urn: str) -> T:
        """Return the :class:`.Enum` member given its `urn`."""
        return cls._member_map_[cls._urn_name[urn]]

    @property
    def urn(self) -> str:
        """Return the URN for an Enum member."""
        for result, name in self._urn_name.items():
            if name == self.name:
                break
        return result


class URNLookupEnum(URNLookupMixin, Enum):
    """Class constructed by ItemSchemeEnumType."""


class ItemSchemeEnumType(EnumType):
    """Metaclass for :class:`.Enum` tied to an SDMX :class:`.ItemScheme`.

    A class constructed using this metaclass **must** have a method
    :py:`_get_item_scheme()` that returns a :class:`sdmx.model.common.ItemScheme`. The
    items in the item scheme become the members of the enumeration.

    Example
    -------
    >>> from message_ix_models.util.sdmx import read
    >>> class EXAMPLE(URNLookupEnum, metaclass=ItemSchemeEnumType):
    ...
    ...     def _get_item_scheme(self):
    ...        return read("AGENCY:CODELIST_ID(1.2.3)")

    …creates a new subclass of :class:`.Enum` that has the methods and properties of
    :class:`.URNLookupMixin`.
    """

    @classmethod
    def __prepare__(metacls, cls, bases, **kwgs):
        return {}

    def __init__(cls, *args, **kwds):
        super(ItemSchemeEnumType, cls).__init__(*args)

    def __new__(metacls, cls, bases, dct, **kwargs) -> type["URNLookupEnum"]:
        # Retrieve the item scheme
        scheme = dct.pop("_get_item_scheme")(None)
        if not isinstance(scheme, common.ItemScheme):
            raise RuntimeError(
                f"Callback for {cls} returned {scheme}; expected ItemScheme"
            )

        # Prepare the EnumDict for creating the class
        enum_dct = super(ItemSchemeEnumType, metacls).__prepare__(cls, bases, **kwargs)
        # Transfer class dct private members
        enum_dct.update(dct)

        # Populate the class member dictionary and URN → member name mapping
        _urn_name = dict()

        if any(issubclass(c, Flag) for c in bases):
            # Ensure the 0 member is NONE, not any of the codes
            enum_dct["NONE"] = 0
        for i, item in enumerate(scheme, start=1):
            _urn_name[item.urn] = item.id
            enum_dct[item.id] = auto()

        # Create the class
        enum_class = cast(
            type["URNLookupEnum"],
            super(ItemSchemeEnumType, metacls).__new__(
                metacls, cls, bases, enum_dct, **kwargs
            ),
        )

        # Store the _urn_name mapping
        setattr(enum_class, "_urn_name", _urn_name)

        return enum_class


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
                v21.Annotation(
                    id=id, text=value if isinstance(value, str) else repr(value)
                )
            )

        result[code.id] = code

    return list(result.values())


def collect_structures(
    target: "sdmx.message.StructureMessage", dfd: "sdmx.model.common.BaseDataflow"
) -> None:
    """Update `target` with `dfd` and related structures.

    These include:

    - The :class:`.DataStructureDefinition` (DSD) that structures `dfd`.
    - For each component (dimension, attribute, or measure) in the DSD:

      - Any :class:`~sdmx.model.common.ConceptScheme` that provides the concept role.
      - Any :class:`~sdmx.model.common.Codelist` that enumerates the component.
    """
    target.add(dfd)
    target.add(dfd.structure)
    for cl in (
        dfd.structure.dimensions,
        dfd.structure.attributes,
        getattr(dfd.structure, "measures", v21.MeasureDescriptor()),
    ):
        for component in cl.components:
            if component.concept_identity is not None:
                target.add(component.concept_identity.parent)
            if component.local_representation is not None:
                target.add(component.local_representation.enumerated)


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


def get(urn: str) -> Optional["common.MaintainableArtefact"]:
    """Return an object given its URN."""
    full_urn = sdmx.urn.expand(urn)
    return STORE.get(sdmx.urn.URN(full_urn).id)


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
        version=get_version(with_dev=False),
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
        is_final=True,
        is_external_reference=False,
    )

    # Add concepts for MESSAGE sets/dimensions
    for k, v in RENAME_DIMS.items():
        # Retrieve or create the Concept for the set (e.g. "year" for k="year_act")
        set_name = k.split("_")[0]
        concept = cs.setdefault(
            id=set_name.upper(),
            name=f"{set_name!r} MESSAGEix set",
            annotations=[v21.Annotation(id="aliases", text=repr(set()))],
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
            v21.Annotation(
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

    STORE.add(cs)

    return cs


@cache
def get_concept(string: str, *, cs_urn: tuple[str, ...] = tuple()) -> "common.Concept":
    """Retrieve (or create) a single Concept from a concept scheme.

    Items are sought first in "CS_MESSAGE_IX_MODELS" (see :func:`get_cs`) and then in
    the concept scheme(s) indicated by `cs_urn`, if any. `string` is matched against
    both item IDS and the contents of an "aliases" annotation (if any).

    If (and only if) `cs_urn` is given **and** the concept is not found, it is created
    in the last of `cs_urn`. In other words, CS_MESSAGE_IX_MODELS is never modified.

    Raises
    ------
    ValueError
        if no concept with an ID or alias matching `string` is found, and `cs_urn` is
        not given.
    """
    # TODO Would prefer to use StructureMessage.get() here, but does not handle URNs.
    #      Adjust once supported upstream
    cs_all = list(
        filter(
            None,
            cast(
                Iterable[Optional["common.ItemScheme"]],
                [get("ConceptScheme=IIASA_ECE:CS_MESSAGE_IX_MODELS")]
                + [get(u) for u in cs_urn],
            ),
        )
    )

    for cs in cs_all:
        for concept in cs.items.values():
            labels = [concept.id] + list(concept.eval_annotation(id="aliases") or [])
            if re.fullmatch("|".join(labels), string, flags=re.IGNORECASE):
                return concept

    if cs_urn:
        return cs_all[-1].setdefault(id=string)
    else:
        raise ValueError(string)


@cache
def get_version(with_dev: Optional[bool] = True) -> str:
    """Return a :class:`sdmx.model.common.Version` for :mod:`message_ix_models`.

    .. todo:: Remove :py:`str(...)` once sdmx1 > 2.21.1 can handle Version.

    Parameters
    ----------
    with_dev : bool
        If :any:`True`, include the dev version part, e.g. "2025.3.12.dev123". If not,
        exclude.
    """
    tmp, *_ = version(__package__.split(".")[0]).partition("+" if with_dev else ".dev")

    return str(common.Version(tmp))


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

    # Ensure URN is populated
    agency.urn = agency.urn or sdmx.urn.make(agency, as_)

    log.info(f"Updated {as_!r}")

    # Write to file again
    write(as_)

    return as_
