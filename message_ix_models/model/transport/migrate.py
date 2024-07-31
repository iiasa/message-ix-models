"""Importing data from MESSAGE (V)-Transport data files.

This code is not currently in use.

See also :mod:`message_data.tools.messagev`.
"""

import logging
from collections import defaultdict
from functools import lru_cache
from itertools import product

import pandas as pd
from tqdm import tqdm

from message_ix_models.tools.messagev import CHNFile, DICFile, INPFile

log = logging.getLogger(__name__)


def import_all(
    context, path, nodes=[], version="geam_ADV3TRAr2_BaseX2_0", verbose=False
):
    """Import data from MESSAGE V files.

    .chn, .dic, and .inp files are read from *path* for the given *nodes*.
    """
    data_path = context.get_path("transport", "migrate")
    data_path.mkdir(exist_ok=True)

    if not (len(nodes) and nodes != [""]):
        nodes = config["MESSAGE V"]["set"]["node"]  # noqa: F821

    # Import .dic and .chn files
    node_path = {n: path / f"{n}_geam.dic" for n in nodes}
    dic = import_dic(node_path)
    del dic  # Unused

    node_path = {n: path / f"{n}_geam.chn" for n in nodes}
    chn = import_chn(node_path)
    del chn  # Unused

    # Import .inp files
    node_path = {n: path / f"{n}_{version}.inp" for n in nodes}
    data = import_inp(node_path, version, verbose)

    log.info("Imported\n" + str(data.head()))
    out_path = data_path / f"{version}.csv.gz"
    log.info(f"Write to {out_path}")

    data.to_csv(data_path / f"{version}.csv.gz")

    # Also write in wide format
    wide = data.reset_index("year").pivot(columns="year")
    wide.to_csv(data_path / f"{version}-wide.csv.gz")

    return data


def import_chn(node_path):
    """Import data from .chn files.

    *node_path* is a mapping from node names to paths of files.

    The tools.messagev.CHNFile class is used to parse the contents.
    """
    return {n: CHNFile(path) for n, path in node_path.items()}


def import_dic(node_path):
    """Import data from .dic files.

    *node_path* is a mapping from node names to paths of files.

    The tools.messagev.DICFile class is used to parse the contents, after which
    the values are merged to a single dictionary.
    """
    # Parse the files
    dics = {}
    for node, path in node_path.items():
        dics[node] = DICFile(path)

    # Merge to a single dictionary
    merged = DICFile()
    for node, dic in dics.items():
        for code, tec in dic.code_tec.items():
            # Assert that the value for this node is the same as others
            assert merged.code_tec.setdefault(code, tec) == tec
            merged.tec_code[tec] = code

    return merged


def import_inp(node_path, version, verbose):
    """Import data from MESSAGE V-Transport .inp files.

    The tools.messagev.INPFile class is used to parse the contents.

    For 11 regions: 4015 region × tec; 357,964 data points.
    """

    # List of all technologies to import
    # TODO re-add old names like Load_factor_truck
    tecs = list(transport_technologies(by_cg=True))  # noqa: F821

    log.info(
        f"Importing from {len(node_path)} nodes × {len(tecs)} technology/"
        "consumer groups"
    )

    @lru_cache()
    def region_file(path):
        """Return the parsed .inp file for region.

        Results are cached, so each file is parsed at most once.
        """
        return INPFile(path)

    # commented: incomplete
    # # Iterate over demand sections
    # for (node, path) in node_path.items():
    #     f = region_file(path)
    #     log.info(f.get_section('demand:'))

    # Prepare an iterator over (node, technology)
    iterator = product(node_path.items(), tecs)
    if not verbose:
        # Show a progress bar
        iterator = tqdm(list(iterator))

    # Accumulate log messages. Emitting these directly while tqdm() is active
    # spoils the progress bar
    _log_messages = []

    def _log(message):
        if not verbose:
            _log_messages.append(message)
        else:
            log.info(message)

    N_dp = 0  # Total number of data points
    data = []  # Loaded data
    no_section = defaultdict(set)  # No section

    # Iterate over regions and technologies
    for (node, path), tec in iterator:
        f = region_file(path)
        try:
            # Get information for this technology in this node
            info = f.parse_section(tec)
        except KeyError:
            no_section[node].add(tec)
            continue
        except Exception as e:
            # Some kind of error while parsing
            _log(f"{e!r} in {node}, {tec}:\n{f.get_section(tec)}")

            # Skip to the next item
            continue

        if "params" not in info:
            _log(f"No data points for {tec} in {node}.\n{f.get_section(tec)}")
            continue

        # Add node, tec columns
        info["params"]["node"] = node
        info["params"]["technology"] = tec

        # Count of data points
        N_dp += len(info["params"])

        data.append(info["params"])

    for node, tecs in no_section.items():
        _log(f"{node}: no section(s) for technologies:\n{tecs!r}")

    # Display accumulated log messages
    log.info("\n---\n".join(_log_messages + [f"{N_dp} data points."]))

    # Concatenate to a single dataframe
    return (
        pd.concat(data)
        .fillna({"source": ""})
        .set_index(["node", "technology", "param", "source", "year"])
    )


def load_all(version):
    """Load cached data from MESSAGE(V) *version*."""
    return load_inp(version)


def load_inp(version):
    """Load cached .inp data for *version*."""
    return pd.read_csv(
        data_path / f"{version}.csv.gz",  # noqa: F821
        index_col="node technology param source year".split(),
    )


# Mapping from .inp lines to MESSAGEix parameters
INP_PARS = {
    # TODO add
    # - 'bda lo' = bound_activity_lo
    "bda": {
        "name": "bound_activity_lo",
    },
    # - 'bda up' = bound_activity_up
    # - 'bdc lo' = bound_new_capacity_lo
    # - 'bdc up' = bound_new_capacity_up
    # - 'mpa lo' = initial_activity_lo, growth_activity_lo
    # - 'mpa up' = initial_activity_up, growth_activity_up
    # - 'mpc lo' = initial_new_capacity_lo, growth_new_capacity_lo
    # - 'mpc up' = initial_new_capacity_up, growth_new_capacity_up
    "plf": {
        "name": "capacity_factor",
        "drop": ["source"],
        "rename": {"node": "node_loc", "year": "year_vtg"},
    },
    "pll": {
        "name": "technical_lifetime",
        "drop": ["source"],
        "rename": {"node": "node_loc", "year": "year_vtg"},
    },
    "minp": {
        "name": "input",
    },
    "moutp": {"name": "output"},
    "fom": {
        "name": "fix_cost",
        "drop": ["source"],
        "rename": {"node": "node_loc", "year": "year_vtg"},
    },
    "inv": {
        "name": "inv_cost",
        "drop": ["source"],
        "rename": {"node": "node_loc", "year": "year_vtg"},
    },
    "vom": {
        "name": "var_cost",
        "drop": ["source"],
        "rename": {"node": "node_loc", "year": "year_vtg"},
    },
}


def fill_year_act(data, info):
    """transform() helper: fill in year_act for *data*."""
    try:
        i = data.index.names.index("year_vtg")
        sort_levels = data.index.names[:i]
    except ValueError:
        return data

    data["year_act"] = data.index.to_frame()["year_vtg"]
    years = list(data["year_act"].unique())
    data = data.set_index("year_act", append=True).unstack("year_act")
    # Additional years to fill that do not appear in year_vtg
    Y = info.Y
    for year in filter(lambda y: y > min(years), sorted(set(Y) - set(years))):
        data[("value", year)] = None

    data = (
        data.fillna(method="ffill", axis=1)
        .stack("year_act")
        .dropna()
        .sort_index(level=sort_levels)
    )

    return data


def truncate(data, info):
    """transform() helper: limit 'year_vtg', 'year' to y0 or later."""
    # Year columns to truncate
    col = next(filter(lambda c: c in data.index.names, ["year_vtg", "year"]))

    log.info("Years " + str(data.index.to_frame()[col].unique()) + f" < {info.y0}")

    return data.query(f"{col} >= {info.y0}")


def transform(data, version, info):
    """Transform *data* from MESSAGE V schema to MESSAGEix.

    Data is written to data_path / version.
    """
    # Create output path
    out_path = data_path / version  # noqa: F821
    out_path.mkdir(exist_ok=True)

    # Rename indices
    data.rename(index=lambda n: f"R11_{n}".upper(), level="node", inplace=True)

    log.info(f"year_vtg >= {info.y0}")

    # Process parameters
    for name, par_info in INP_PARS.items():
        # - Select data for this parameter.
        # - Drop the column 'param' and any others.
        # - Rename MultiIndex levels.
        # - Fill in 'year_act' from 'year_vtg'.
        # - Truncate pre-model years.
        par = (
            data.query(f"param == '{name}'")
            .droplevel(["param"] + par_info.get("drop", []))
            .rename_axis(index=par_info.get("rename", {}))
            .pipe(fill_year_act, info)
            .pipe(truncate, info)
        )

        log.info(f"{len(par)} rows in {name}/{par_info['name']}")

        # Write to file
        par.to_csv(out_path / f"{par_info['name']}.csv")


def plot_inp_data(data, target_path):
    """Quick diagnostic plots of .inp file data."""
    import plotnine as p9

    plot = (
        p9.ggplot(data.reset_index(), p9.aes(x="year", y="value", color="node"))
        + p9.geom_point()
    )
    plot.save(target_path / "demo.pdf")
