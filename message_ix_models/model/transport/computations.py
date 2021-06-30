import pandas as pd

from genno import Quantity, computations
from ixmp.reporting import RENAME_DIMS
from message_ix_models import ScenarioInfo
from message_ix_models.util import private_data_path


def load_transport_file(
    basename: str, units=None, name: str = None, dims=[]
) -> Quantity:
    """Load transport calibration data from a CSV file.

    Wrapper around :func:`genno.computations.load_file`.

    Parameters
    ----------
    basename : str
        Base name of the file, excluding the :file:`.csv` suffix and the path. The
        full path is constructed automatically using :func:`.private_data_path`.
    units : str or pint.Unit, optional
        Units to assign the the resulting
    name : str, optional
        Name to assign.
    """

    _dims = {d: d for d in dims}
    _dims.update(RENAME_DIMS)

    return computations.load_file(
        path=private_data_path("transport", basename).with_suffix(".csv"),
        dims=_dims,
        units=units,
        name=name,
    )


def as_quantity(info):
    dim = info.pop("_dim")
    unit = info.pop("_unit")

    return Quantity(pd.Series(info).rename_axis(dim), units=unit)


def ldv_distance(config):
    """Return annual driving distance per LDV.

    - Regions other than R11_NAM have M/F values in same proportion to their A value as
      in NAM
    """
    # Load from config.yaml
    result = computations.product(
        as_quantity(config["ldv activity"]),
        as_quantity(config["factor"]["activity"]["ldv"]),
    )

    result.name = "ldv distance"

    return result


def transport_check(scenario, ACT):
    """Reporting computation for :func:`check`.

    Imported into :mod:`.reporting.computations`.
    """
    info = ScenarioInfo(scenario)

    # Mapping from check name → bool
    checks = {}

    # Correct number of outputs
    ACT_lf = ACT.sel(t=["transport freight load factor", "transport pax load factor"])
    checks["'transport * load factor' technologies are active"] = len(
        ACT_lf
    ) == 2 * len(info.Y) * (len(info.N) - 1)

    # # Force the check to fail
    # checks['(fail for debugging)'] = False

    return pd.Series(checks)
