import logging
from collections import defaultdict
from copy import copy
from functools import partial
from itertools import product
from typing import List, Mapping, MutableMapping, Sequence, Union

import message_ix
import pandas as pd
from sdmx.model.v21 import Annotation, Code

from message_ix_models import ScenarioInfo, Spec
from message_ix_models.model.build import apply_spec
from message_ix_models.util import (
    broadcast,
    eval_anno,
    make_io,
    make_matched_dfs,
    make_source_tech,
    merge_data,
    nodes_ex_world,
    same_node,
)

log = logging.getLogger(__name__)

CodeLike = Union[str, Code]


def add(
    scenario: message_ix.Scenario,
    groups: Sequence[Code],
    technologies: Sequence[Code],
    template: Code,
    **options,
) -> Spec:
    """Add disutility formulation to `scenario`."""
    # Generate the spec given the configuration options
    spec = get_spec(groups, technologies, template)

    # Apply spec and add data
    apply_spec(scenario, spec, partial(get_data, spec=spec), **options)

    return spec


def get_spec(
    groups: Sequence[Code], technologies: Sequence[Code], template: Code
) -> Spec:
    """Get a spec for a disutility formulation.

    Parameters
    ----------
    groups : list of |Code|
        Identities of the consumer groups with distinct disutilities.
    technologies : list of |Code|
        The technologies to which the disutilities are applied.
    template : |Code|

    """
    s = Spec()

    s.require.set["technology"].extend(technologies)

    # Disutility commodity and source
    s.add.set["commodity"] = [Code(id="disutility")]
    s.add.set["technology"] = [Code(id="disutility source")]

    # Disutility is unitless
    # NB this value is currently ignored by .build.apply_spec(). See #45.
    s.add.set["unit"].append("")

    # Unrelated annotations in the template
    other_anno = list(
        filter(lambda a: a.id not in ("input", "output"), template.annotations)
    )

    # Add conversion technologies
    for t, g in product(technologies, groups):
        # String formatting arguments
        fmt = dict(technology=t, group=g)

        # Format each field in the "input" and "output" annotations
        input = {k: v.format(**fmt) for k, v in eval_anno(template, id="input").items()}
        output = {
            k: v.format(**fmt) for k, v in eval_anno(template, id="output").items()
        }

        # - Format the ID string from the template
        # - Create new "input" and "output" annotations
        # - Copy other annotations unmodified
        t_code = Code(
            id=template.id.format(**fmt),
            annotations=[
                Annotation(id="input", text=repr(input)),
                Annotation(id="output", text=repr(output)),
            ]
            + [copy(a) for a in other_anno],
        )

        # "commodity" set elements to add
        s.add.set["commodity"].extend([input["commodity"], output["commodity"]])

        # "technology" set elements to add
        t_code.annotations.append(Annotation(id="input", text=repr(input)))
        s.add.set["technology"].append(t_code)

    # Deduplicate "commodity" set elements
    s.add.set["commodity"] = sorted(map(str, set(s.add.set["commodity"])))

    return s


def get_data(scenario, spec, **kwargs) -> Mapping[str, pd.DataFrame]:
    """Get data for disutility formulation.

    Calls :meth:`data_conversion` and :meth:`data_source`.

    Parameters
    ----------
    spec : dict
        The output of :meth:`get_spec`.
    """
    if len(kwargs):
        log.warning(f"Ignore {repr(kwargs)}")

    info = ScenarioInfo(scenario)

    # Get conversion technology data
    data = data_conversion(info, spec)

    # Get and append source data
    merge_data(data, data_source(info, spec))

    return data


def dp_for(col_name: str, info: ScenarioInfo) -> pd.Series:  # pragma: no cover
    """:meth:`pandas.DataFrame.assign` helper for ``duration_period``.

    Returns a callable to be passed to :meth:`pandas.DataFrame.assign`. The callable
    takes a data frame as the first argument, and returns a :class:`pandas.Series`
    based on the ``duration_period`` parameter in `info`, aligned to `col_name` in the
    data frame.

    Currently (2021-04-07) unused.
    """

    def func(df):
        return df.merge(info.par["duration_period"], left_on=col_name, right_on="year")[
            "value_y"
        ]

    return func


def data_conversion(info, spec) -> MutableMapping[str, pd.DataFrame]:
    """Generate input and output data for disutility conversion technologies."""
    common = dict(
        mode="all",
        year_vtg=info.Y,
        year_act=info.Y,
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    # Use the spec to retrieve information
    technology = spec["add"].set["technology"]

    # Data to return
    data0: Mapping[str, List[pd.DataFrame]] = defaultdict(list)

    # Loop over conversion technologies
    for t in technology:
        # Use the annotations on the technology Code to get information about the
        # commodity, level, and unit
        input = eval_anno(t, "input")
        output = eval_anno(t, "output")
        if None in (input, output):
            if t.id == "disutility source":
                continue  # Data for this tech is from data_source()
            else:  # pragma: no cover
                raise ValueError(t)  # Error in user input

        # Make input and output data frames
        i_o = make_io(
            (input["commodity"], input["level"], input["unit"]),
            (output["commodity"], output["level"], output["unit"]),
            1.0,
            on="output",
            technology=t.id,
            **common,
        )
        for par, df in i_o.items():
            # Broadcast across nodes
            df = df.pipe(broadcast, node_loc=nodes_ex_world(info.N)).pipe(same_node)

            if par == "input":
                # Add input of disutility
                df = pd.concat(
                    [df, df.assign(commodity="disutility", unit="-")], ignore_index=True
                )

            data0[par].append(df)

    # Concatenate to a single data frame per parameter
    data = {par: pd.concat(dfs, ignore_index=True) for par, dfs in data0.items()}

    # Create data for capacity_factor
    data.update(make_matched_dfs(base=data["input"], capacity_factor=1.0))

    return data


def data_source(info, spec) -> Mapping[str, pd.DataFrame]:
    """Generate data for a technology that emits the “disutility” commodity."""
    # List of input levels where disutility commodity must exist
    levels = set()
    for t in spec["add"].set["technology"]:
        input = eval_anno(t, "input")
        if input:
            levels.add(input["level"])

    log.info(f"Generate disutility on level(s): {repr(levels)}")

    # Use default capacity_factor = 1.0
    result = make_source_tech(
        info,
        common=dict(
            commodity="disutility",
            mode="all",
            technology="disutility source",
            time="year",
            time_dest="year",
            unit="-",
        ),
        output=1.0,
        var_cost=1.0,
    )
    result["output"] = result["output"].pipe(broadcast, level=sorted(levels))

    return result
