from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click

from message_ix_models.util.click import common_params

if TYPE_CHECKING:
    from genno.types import AnyQuantity

    from message_ix_models import Context


@click.group("ssp")
def cli():
    """Shared Socioeconomic Pathways (SSP) project."""


@cli.command("gen-structures")
@common_params("dry_run")
@click.pass_obj
def gen_structures(context, **kwargs):
    """(Re)Generate the SSP data structures in SDMX."""
    from .structure import generate

    generate(context)


@cli.command("transport")
@click.option("--method", type=click.Choice(["A", "B", "C"]), required=True)
@click.argument("path_in", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument(
    "path_out",
    type=click.Path(exists=False, dir_okay=False, path_type=Path),
    required=False,
)
@click.pass_obj
def transport_cmd(context: "Context", method, path_in: Path, path_out: Optional[Path]):
    """Postprocess aviation emissions.

    Data are read from PATH_IN, in .xlsx or .csv format. If .xlsx, the data are first
    temporarily converted to .csv. Data are written to PATH_OUT; if not given, this
    defaults to the same path and suffix as PATH_IN, with "_out" added to the stem.

    For --method=C, the top-level option --platform=ixmp-dev (for example) may be used
    to specify the Platform on which to locate solved MESSAGEix-Transport scenarios.
    """
    import pandas as pd
    from platformdirs import user_cache_path

    from .transport import METHOD, process_file

    if path_in.suffix == ".xlsx":
        path_in_user = path_in
        path_in = user_cache_path("message-ix-models").joinpath(path_in.stem + ".csv")
        print(f"Convert Excel input to {path_in}")
        pd.read_excel(path_in_user).to_csv(path_in, index=False)
    else:
        path_in_user = path_in

    if path_out is None:
        path_out = path_in_user.with_name(
            path_in_user.stem + "_out" + path_in_user.suffix
        )
        print(f"No PATH_OUT given; write to {path_out}")

    if path_out.suffix == ".xlsx":
        path_out_user = path_out
        path_out = user_cache_path("message-ix-models").joinpath(path_out.stem + ".csv")
    else:
        path_out_user = path_out

    process_file(
        path_in,
        path_out,
        method=METHOD[method],
        platform_name=context.core.platform_info.get("name", None),
    )

    if path_out_user != path_out:
        print(f"Convert CSV output to {path_out_user}")
        pd.read_csv(path_out).to_excel(path_out_user, index=False)


#: A previous version of this file was named "ceds_cmip7_intlAviationShipping_0010.csv".
CEDS_FILENAME = "ceds_cmip7_Aircraft_intlShipping_byfuel_v_2025_03_18.csv"


# NB No test coverage because this requires access to the ixmp-dev database, which is
#    not available from GitHub-hosted runners
@cli.command("ceds-data-for-transport")
@click.argument("path_in", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.pass_obj
def ceds_data_for_transport(
    context: "Context", path_in: Path
) -> None:  # pragma: no cover
    f"""Compute transport emisions intensity from CEDS data.

    This code uses data in data/ceds/{CEDS_FILENAME} for total emissions and divides
    by total aviation final energy that would be computed from PATH_IN using method 'C'.

    The resulting emission factors should exactly recover the CEDS totals.
    """
    import genno
    import pandas as pd

    from message_ix_models.tools.iamc import iamc_like_data_for_query
    from message_ix_models.util import package_data_path

    from .transport import IAMC_KW, METHOD, K, get_computer

    # The following lines identical to .transport.process_file()
    row0 = pd.read_csv(path_in, nrows=1).iloc[0, :]
    c = get_computer(row0, METHOD.C)
    c.add(K.input, iamc_like_data_for_query, path=path_in, **IAMC_KW)

    # Extend `c` for the calculation

    def numerator() -> "AnyQuantity":
        """Numerator for emissions intensity: total emissions."""
        path = package_data_path("ceds", CEDS_FILENAME)
        df = (
            pd.read_csv(path)
            .assign(mask=lambda df: df.variable.str.match(r".*Aircraft\|light_oil"))
            .query("mask")[["region", "variable", "unit", "2019", "2020"]]
        )
        # Show 2019 and 2020 values
        print(f"Read from {path}:\n{df.to_string()}")

        return genno.Quantity(df.set_index("variable")["2019"], units="Mt/a")

    c.add("numerator:variable", numerator)

    # Denominator for emission intensity: total final energy

    # # - Use an intermediate quantity: numerator for FE share == aviation total FE
    # #   according to MESSAGEix-Transport
    # # - Select on all 3 dimensions, giving a 0-D quantity/scalar
    # indexers = dict(c="lightoil", nl="World", ya=2020)
    # c.add("denom::0", "select", "fe share:c-nl-ya:AIR emi+num", indexers=indexers)

    # - Use an intermediate quantity: aviation total FE computed as a share of base
    #   MESSAGEix-GLOBIOM transport final energy
    # - Select on all 3 dimensions, giving an 0-D quantity/scalar
    indexers = dict(c="lightoil", n="World", y=2020)
    c.add("denom::0", "select", "fe:c-n-y:AIR emi+BC", indexers=indexers)

    # Convert from GWa/a to MJ/a
    c.add("denom::1", "convert_units", "denom::0", units="MJ / a")

    # Implied emission factor = total emissions ÷ total final energy
    c.add("result:variable:0", "div", "numerator:variable", "denom::1")
    # Convert to units matching emi-intensity.csv
    c.add("result:variable", "convert_units", "result:variable:0", units="g/MJ")

    # Show what will be done
    # print(c.describe("result:variable"))

    # Compute and show the result
    result = c.get("result:variable")
    print(result.to_string(), f"{result.units = }", sep="\n")
