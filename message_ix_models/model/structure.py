import logging
import re
from collections import ChainMap
from copy import copy
from functools import lru_cache
from itertools import product
from typing import Dict, List, Mapping, MutableMapping, Tuple

import click
import pandas as pd
import pycountry
import xarray as xr
from iam_units import registry
from sdmx.model.v21 import Annotation, Code

from message_ix_models.util import eval_anno, load_package_data, package_data_path
from message_ix_models.util.sdmx import as_codes

log = logging.getLogger(__name__)


@lru_cache()
def codelists(kind: str) -> List[str]:
    """Return a valid IDs for code lists of `kind`.

    Parameters
    ----------
    kind : str
        "node" or "year".
    """
    return sorted(path.stem for path in package_data_path(kind).glob("*.yaml"))


@lru_cache()
def get_codes(name: str) -> List[Code]:
    """Return codes for the dimension/set `name` in MESSAGE-GLOBIOM scenarios.

    The information is read from :file:`data/{name}.yaml`, e.g.
    :file:`data/technology.yaml`.

    When `name` includes "node", then child codes are automatically populated from the
    ISO 3166 database via :mod:`pycountry`. For instance:

    .. code-block:: yaml

       myregion:
         name: Custom region
         child: [AUT, SCG]

    …results in a region with child codes for Austria (a current country) and the
    formerly-existing country Serbia and Montenegro.

    Parameters
    ----------
    name : :class:`str`
        Any :file:`.yaml` file in the folder :file:`message_ix_models/data/`.

    Returns
    -------
    list of :class:`~sdmx.model.Code`
        Every Code has :attr:`id`, :attr:`name`, :attr:`description`, and
        :attr:`annotations` attributes. Calling :func:`str` on a code returns its
        :attr:`id`.
    """
    # Raw contents of the config file
    config = load_package_data(name)

    if "node" in name:
        # Automatically add information for countries within regions in the node
        # codelists. Use a ChainMap to combine a the `config` loaded from file and then
        # fall back to contents of the pycountry databases.
        config = ChainMap(
            config,
            # Create codes using the ISO database via pycountry
            {c.alpha_3: dict(id=c.alpha_3, name=c.name) for c in pycountry.countries},
            # Also include historic countries
            {
                c.alpha_3: dict(id=c.alpha_3, name=c.name)
                for c in pycountry.historic_countries
            },
        )

    # Convert to codes
    data = as_codes(config)

    # Fill in additional data, defaults, etc.
    if name == "commodity":
        process_commodity_codes(data)
    elif name == "technology":
        process_technology_codes(data)

    return data


@lru_cache()
def get_region_codes(codelist: str) -> List[Code]:
    """Return the codes that are children of "World" in the specified `codelist`."""
    nodes = get_codes(f"node/{codelist}")
    return nodes[nodes.index(Code(id="World"))].child


def generate_product(
    data: Mapping, name: str, template: Code
) -> Tuple[List[Code], Dict[str, xr.DataArray]]:
    """Generates codes using a `template` by Cartesian product along ≥1 dimensions.

    :func:`generate_set_elements` is called for each of the `dims`, and these values
    are used to format `base`.

    Parameters
    ----------
    data
        Mapping from dimension IDs to lists of codes.
    name : str
        Name of the set.
    template : Code
        Must have Python format strings for its its :attr:`id` and :attr:`name`
        attributes.
    """
    # eval() and remove the original annotation
    dims = eval_anno(template, "_generate")
    template.pop_annotation(id="_generate")

    def _base(dim, match):
        """Return codes along dimension `dim`.

        If `match` is given, only children matching an expression."""
        dim_codes = data[dim]["add"]

        try:
            i = dim_codes.index(match)
        except ValueError:
            if isinstance(match, str):
                expr = re.compile(match)
                dim_codes = list(filter(lambda c: expr.match(c.id), dim_codes))
        else:
            dim_codes = dim_codes[i].child

        return dim_codes

    codes = []  # Accumulate codes and indices
    indices = []

    # Iterate over the product of filtered codes for each dimension in
    for item in product(*[_base(*dm) for dm in dims.items()]):
        result = copy(template)  # Duplicate the template

        fmt = dict(zip(dims.keys(), item))  # Format the ID and name
        result.id = result.id.format(**fmt)
        result.name = str(result.name).format(**fmt)  # type: ignore [assignment]

        codes.append(result)  # Store code and indices
        indices.append(tuple(map(str, item)))

    # - Convert length-N sequence of D-tuples to D iterables each of length N.
    # - Convert to D × 1-dimensional xr.DataArrays, each of length N.
    tmp = zip(*indices)
    indexers = {d: xr.DataArray(list(i), dims=name) for d, i in zip(dims.keys(), tmp)}
    # Corresponding indexer with the full code IDs
    indexers[name] = xr.DataArray([c.id for c in codes], dims=name)

    return codes, indexers


def generate_set_elements(data: MutableMapping, name) -> None:
    """Generate elements for set `name`.

    This function converts lists of codes in `data`, calling :func:`generate_product`
    and :func:`process_units_anno` as appropriate.

    Parameters
    ----------
    data
        Mapping from dimension IDs to lists of codes.
    name : str
        Name of the set for which to generate elements e.g. "commodity" or "technology".
    """
    hierarchical = name in {"technology"}

    codes = []  # Accumulate codes
    deferred = []
    for code in as_codes(data[name].get("add", [])):
        if name in {"commodity", "technology"}:
            process_units_anno(name, code, quiet=True)

        if eval_anno(code, "_generate"):
            # Requires a call to generate_product(); do these last
            deferred.append(code)
            continue

        codes.append(code)

        if hierarchical:
            # Store the children of `code`
            codes.extend(filter(lambda c: c not in codes, code.child))

    # Store codes processed so far, in case used recursively by generate_product()
    data[name]["add"] = codes

    # Use generate_product() to generate codes and indexers based on other sets
    for code in deferred:
        generated, indexers = generate_product(data, name, code)

        # Store
        data[name]["add"].extend(generated)

        # NB if there are >=2 generated groups, only indexers for the last are kept
        data[name]["indexers"] = indexers


def process_units_anno(set_name: str, code: Code, quiet: bool = False) -> None:
    """Process an annotation on `code` with id="units".

    The annotation text is wrapped as ``'registry.Unit("{text}")'``, such that it can
    be retrieved with :func:`.eval_anno` or :meth:`.ScenarioInfo.units_for`. If `code`
    has direct children, the annotation is also copied to those codes.

    Parameters
    ----------
    set_name : str
        Used in logged messages when `quiet` is :data:`False`.
    quiet : bool, *optional*
        If :data:`False` (the default), log on level :ref:`WARNING <python:levels>` if:

        - the annotation is missing, or
        - its text is not parseable with the :mod:`pint` application registry, i.e.
          :data:`iam_units.registry`.

        Otherwise, log on :ref:`DEBUG <python:levels>`.

    """
    level = logging.DEBUG if quiet else logging.WARNING
    # Convert a "units" annotation to a code snippet that will return a pint.Unit
    # via eval_anno()
    try:
        units_anno = code.get_annotation(id="units")
    except KeyError:
        log.log(level, f"{set_name.title()} {code} lacks defined units")
        return

    # First try the expression as-is, in case already processed
    expr = None
    for candidate in (str(units_anno.text), f'registry.Unit("{units_anno.text}")'):
        # Check that the unit can be parsed by the pint.UnitRegistry
        try:
            result = eval(candidate)
        except Exception:
            continue
        else:
            if isinstance(result, registry.Unit):
                expr = candidate
                break

    if not expr:  # pragma: no cover
        # No coverage: code that triggers this exception should never be committed
        log.log(
            level,
            f"Unit '{units_anno.text}' for {set_name} {code} not pint compatible",
        )
    else:
        # Modify the annotation so eval_anno() can be used
        units_anno.text = expr

    # Also annotate child codes
    for c in code.child:
        c.annotations.append(copy(units_anno))


def process_commodity_codes(codes):
    """Process a list of codes for ``commodity``.

    The function warns for commodities missing units or with non-:mod:`pint`-compatible
    units.
    """
    for code in codes:
        # FIXME remove quiet=True; instead improve commodity.yaml with units
        process_units_anno("commodity", code, quiet=True)


def process_technology_codes(codes):
    """Process a list of codes for ``technology``.

    This function ensures every code has an annotation with id "vintaged", default
    :obj:`False`.
    """
    for code in codes:
        # FIXME remove quiet=True; instead improve technology.yaml with units
        process_units_anno("technology", code, quiet=True)

        try:
            anno = code.pop_annotation(id="vintaged")
        except KeyError:
            # Default value for 'vintaged'
            anno = Annotation(id="vintaged", text=repr(False))

        code.annotations.append(anno)


@click.command(name="techs")
@click.pass_obj
def cli(ctx):
    """Export metadata to technology.csv.

    This command transforms the technology metadata from the YAML file to CSV format.
    """
    # Convert each code to a pd.Series
    data = []
    for code in get_codes("technology"):
        # Base attributes
        d = dict(id=code.id, name=str(code.name), description=str(code.description))

        # Annotations
        for anno in ("type", "vintaged", "sector", "input", "output"):
            try:
                d[anno] = str(code.get_annotation(id=anno).text)
            except KeyError:
                pass

        data.append(pd.Series(d))

    # Combine series to a data frame
    techs = pd.DataFrame(data)

    # Write to file
    dest = ctx.get_local_path("technology.csv")
    print(f"Write to {dest}")

    techs.to_csv(dest, index=None, header=True)

    # Print the first few items of the data frame
    print(techs.head())
