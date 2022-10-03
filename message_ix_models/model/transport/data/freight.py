"""Freight transport data."""
from collections import defaultdict
from typing import Dict, List

import pandas as pd
from genno import Quantity, computations
from genno.computations import broadcast_map, load_file
from iam_units import registry
from ixmp.reporting.computations import map_as_qty
from message_ix_models import Context
from message_ix_models.model.structure import get_codes
from message_ix_models.util import (
    broadcast,
    make_io,
    make_matched_dfs,
    private_data_path,
    same_node,
)

from message_data.model.transport.utils import input_commodity_level


def iea_2017_t4(measure: int):
    """Retrieve IEA “Future of Trucks” data.

    Parameters
    ----------
    measure : int
        One of:

        1. energy intensity of vehicle distance travelled
        2. load
        3. energy intensity of freight service (mass × distance)
    """
    data = load_file(
        private_data_path("transport", f"iea-2017-t4-{measure}.csv"),
        dims={"node": "n", "technology": "t"},
    )

    # Broadcast to regions
    m = pd.DataFrame(
        [
            ("R12_CHN", "CHN"),
            ("R12_EEU", "EU28"),
            ("R12_NAM", "USA"),
            ("R12_SAS", "IND"),
            ("R12_WEU", "EU28"),
            # Assumed similarity
            ("R12_AFR", "IND"),
            ("R12_FSU", "CHN"),
            ("R12_LAM", "USA"),
            ("R12_MEA", "CHN"),
            ("R12_PAO", "CHN"),
            ("R12_PAS", "CHN"),
            ("R12_RCPA", "CHN"),
        ],
        columns=["n2", "n"],
    )
    m = map_as_qty(m[["n", "n2"]], [])  # Expects columns in the from/to order
    result = broadcast_map(data, m, rename={"n2": "n"})

    # Share of freight activity; transcribed from figure 18, page 38
    share = Quantity(
        pd.Series([0.1, 0.3, 0.6], index=pd.Index("LCV MFT HFT".split(), name="t"))
    )
    return computations.sum(result, share, "t")


def get_freight_data(
    nodes: List[str], years: List[int], context: Context
) -> Dict[str, pd.DataFrame]:
    """Data for freight technologies.

    See also
    --------
    strip_emissions_data
    """
    # Info about the model structure being built
    info = context["transport spec"].add
    info.set["commodity"].extend(get_codes("commodity"))
    codes = info.set["technology"]
    technologies = codes[codes.index("freight truck")]

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
    efficiency = (
        computations.convert_units(iea_2017_t4(1), "GWa / (Gv km)")
        .to_series()
        .reset_index()
        .set_axis(["node_loc", "value"], axis=1)
    )

    # NB currently unused. These values are in the range of 10–12 tonne / vehicle.
    load_factor = (
        iea_2017_t4(2).to_series().reset_index().set_axis(["node_loc", "value"], axis=1)
    )
    del load_factor

    data0: Dict[str, List] = defaultdict(list)
    for t in technologies.child:
        units_in = info.io_units(t, "lightoil")
        units_out = info.io_units(t, "transport freight vehicle")
        i_o = make_io(
            src=(None, None, f"{units_in:~}"),
            dest=("transport freight vehicle", "useful", f"{units_out:~}"),
            efficiency=None,
            on="input",
            technology=t.id,
            **common,
        )

        data0["input"].append(
            input_commodity_level(i_o["input"], "final", context=context)
            .pipe(broadcast, efficiency)
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

    # TODO re-add emissions data from file

    return data1
