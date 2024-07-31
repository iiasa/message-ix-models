"""Freight transport data."""

from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List

import pandas as pd
from genno import Computer
from iam_units import registry
from sdmx.model.v21 import Code

from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import broadcast, make_io, make_matched_dfs, same_node

from .util import input_commodity_level

if TYPE_CHECKING:
    from genno import Quantity


def prepare_computer(c: Computer):
    k = "transport freight::ixmp"
    c.add(
        k,
        get_freight_data,
        "energy intensity of VDT:n-y",
        "n::ex world",
        "t::transport",
        "y::model",
        "context",
    )
    c.add("transport_data", __name__, key=k)


def get_freight_data(
    efficiency: "Quantity",
    nodes: List[str],
    techs: List[Code],
    years: List[int],
    context: Context,
) -> Dict[str, pd.DataFrame]:
    """Data for freight technologies.

    See also
    --------
    strip_emissions_data
    """
    # Info about the model structure being built
    info = context.transport.spec.add
    info.set["commodity"].extend(get_codes("commodity"))

    technologies = techs[techs.index(Code(id="freight truck"))].child

    # Information for data creation
    common = dict(
        year_vtg=years,
        year_act=years,
        mode="all",
        time="year",  # no subannual detail
        time_dest="year",
        time_origin="year",
    )

    # Efficiency information
    efficiency_df = (
        efficiency.to_series().reset_index().set_axis(["node_loc", "value"], axis=1)
    )

    # # NB currently unused. These values are in the range of 10–12 tonne / vehicle.
    # load_factor = (
    #     iea_2017_t4(2).to_series().reset_index()
    #     .set_axis(["node_loc", "value"], axis=1)
    # )
    # del load_factor

    cl_veh = ("transport freight road vehicle", "useful")

    data0: Dict[str, List] = defaultdict(list)
    for t in technologies:
        units_in = info.io_units(t, "lightoil")
        units_out = info.io_units(t, cl_veh[0])
        i_o = make_io(
            src=(None, None, f"{units_in:~}"),
            dest=(*cl_veh, f"{units_out:~}"),
            efficiency=None,
            on="input",
            technology=t.id,
            **common,
        )

        data0["input"].append(
            input_commodity_level(context, i_o["input"], default_level="final")
            .pipe(broadcast, efficiency_df)
            .pipe(same_node)
        )
        data0["output"].append(
            i_o["output"].pipe(broadcast, node_loc=nodes).pipe(same_node)
        )

    data1: Dict[str, pd.DataFrame] = {par: pd.concat(dfs) for par, dfs in data0.items()}

    data1.update(
        make_matched_dfs(
            base=data1["input"],
            capacity_factor=registry.Quantity("1"),
            technical_lifetime=registry("10 year"),
        )
    )

    # Conversion technology
    for par, df in make_io(
        (*cl_veh, "Gv km"),
        ("transport freight road", "useful", "Gt km"),
        context.transport.load_factor["freight"],
        on="output",
        technology="transport freight road usage",
        **common,
    ).items():
        data1[par] = pd.concat(
            [data1[par], df.pipe(broadcast, node_loc=nodes).pipe(same_node)]
        )

    # TODO re-add emissions data from file

    return data1
