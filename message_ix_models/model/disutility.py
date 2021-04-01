import logging
from collections import defaultdict
from functools import lru_cache, partial
from typing import Dict, List, Mapping, Sequence, Union

import message_ix
import pandas as pd
from sdmx.model import Annotation, Code

from message_ix_models import ScenarioInfo
from message_ix_models.model.build import apply_spec
from message_ix_models.util import (
    broadcast,
    eval_anno,
    make_io,
    make_matched_dfs,
    make_source_tech,
    merge_data,
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
) -> None:
    """Add disutility formulation to `scenario`."""
    # Generate the spec given the configuration options
    spec = get_spec(groups, technologies, template)

    # Apply spec and add data
    apply_spec(scenario, spec, partial(get_data, spec=spec), **options)


def get_spec(
    groups: Sequence[Code],
    technologies: Sequence[Code],
    template: Code,
) -> Dict[str, ScenarioInfo]:
    """Get a spec for a disutility formulation.

    Parameters
    ----------
    groups : list of Code
        Identities of the consumer groups with distinct disutilities.
    technologies : list of Code
        The technologies to which the disutilities are applied.
    template : .Code

    """
    require = ScenarioInfo()
    remove = ScenarioInfo()
    add = ScenarioInfo()

    require.set["technology"].extend(technologies)

    # Disutility commodity and source
    add.set["commodity"] = [Code(id="disutility")]
    add.set["technology"] = [Code(id="disutility source")]

    # Add consumer groups
    add.set["mode"].extend(Code(id=g.id, name=f"Production for {g.id}") for g in groups)

    # Add conversion technologies
    for t in technologies:
        # String formatting arguments
        fmt = dict(technology=t)

        # Format each field in the "input" and "output" annotations
        input = {k: v.format(**fmt) for k, v in eval_anno(template, id="input").items()}
        output = eval_anno(template, id="output")

        # - Format the ID string from the template
        # - Copy the "output" annotation without modification
        t_code = Code(
            id=template.id.format(**fmt),
            annotations=[
                template.get_annotation(id="output"),
                Annotation(id="input", text=repr(input)),
            ],
        )

        # "commodity" set elements to add
        add.set["commodity"].append(input["commodity"])
        add.set["commodity"].extend(
            output["commodity"].format(mode=g.id) for g in groups
        )

        # "technology" set elements to add
        t_code.annotations.append(Annotation(id="input", text=repr(input)))
        add.set["technology"].append(t_code)

    # Deduplicate "commodity" set elements
    add.set["commodity"] = sorted(map(str, set(add.set["commodity"])))

    return dict(require=require, remove=remove, add=add)


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


def data_conversion(info, spec) -> Mapping[str, pd.DataFrame]:
    """Input and output data for disutility conversion technologies."""
    common = dict(
        year_vtg=info.Y,
        year_act=info.Y,
        # No subannual detail
        time="year",
        time_origin="year",
        time_dest="year",
    )

    # Use the spec to retrieve information
    technology = spec["add"].set["technology"]
    mode = list(map(str, spec["add"].set["mode"]))

    # Data to return
    data0: Mapping[str, List[pd.DataFrame]] = defaultdict(list)

    # Loop over technologies
    for t in technology:
        # Use the annotations on the technology Code to get information about the
        # commodity, level, and unit
        input = eval_anno(t, "input")
        output = eval_anno(t, "output")
        if input is output is None:
            if t.id == "disutility source":
                continue  # Data for this tech is from disutility_source()
            else:
                raise ValueError(t)  # Error in user input

        # Helper functions for output
        @lru_cache()
        def oc_for_mode(mode):
            # Format the output commodity id given the mode id
            return output["commodity"].format(mode=mode)

        def output_commodity(df):
            # Return a series with output commodity based on mode
            return df["mode"].apply(oc_for_mode)

        # Make input and output data frames
        i_o = make_io(
            (input["commodity"], input["level"], input["unit"]),
            (None, output["level"], output["unit"]),
            1.0,
            on="output",
            technology=t.id,
            **common,
        )
        for par, df in i_o.items():
            # Broadcast across nodes
            df = df.pipe(broadcast, node_loc=info.N[1:]).pipe(same_node)
            if par == "input":
                # Common across modes
                data0[par].append(df.assign(mode="all"))

                # Disutility inputs differ by mode
                data0[par].append(
                    df.assign(commodity="disutility").pipe(broadcast, mode=mode)
                )
            elif par == "output":
                # - Broadcast across modes
                # - Use a function to set the output commodity based on the
                #   mode
                data0[par].append(
                    df.pipe(broadcast, mode=mode).assign(commodity=output_commodity)
                )

    # Concatenate to a single data frame per parameter
    data = {par: pd.concat(dfs) for par, dfs in data0.items()}

    # Create data for capacity_factor and technical_lifetime
    data.update(
        make_matched_dfs(
            base=data["input"],
            capacity_factor=1,
            # TODO get this from ScenarioInfo
            technical_lifetime=10,
            # commented: activity constraints for the technologies
            # TODO get these values from an argument
            growth_activity_lo=-0.5,
            # growth_activity_up=0.5,
            # initial_activity_up=1.,
            # soft_activity_lo=-0.5,
            # soft_activity_up=0.5,
        )
    )
    # Remove growth_activity_lo for first year
    data["growth_activity_lo"] = data["growth_activity_lo"].query(
        f"year_act > {spec['add'].y0}"
    )

    # commented: initial activity constraints for the technologies
    # data.update(
    #    make_matched_dfs(base=data["output"], initial_activity_up=2.)
    # )

    return data


def data_source(info, spec) -> Mapping[str, pd.DataFrame]:
    """Generate data for a technology that emits the disutility commodity."""
    # List of input levels where disutility commodity must exist
    levels = set()
    for t in spec["add"].set["technology"]:
        input = eval_anno(t, "input")
        if input:
            levels.add(input["level"])
        else:
            # "disutility source" technology has no annotations
            continue

    log.info(f"Generate disutility on level(s): {repr(levels)}")

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
        # TODO get this from ScenarioInfo
        technical_lifetime=10,
    )
    result["output"] = result["output"].pipe(broadcast, level=sorted(levels))

    return result
