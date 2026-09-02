"""Configuration for :class:`.report.legacy`.

Classes and functions in this module mimic behaviour of configuration-handling code in
:func:`.legacy.iamc_report_hackathon.main`. However, they are **not** used there. They
are only used by :mod:`.legacy.compat` to mimic the behaviour of iamc_report_hackathon
within a :class:`.Reporter`.

Theses classes isolate config-handling logic, default, and file formats from the final
information actually used in reporting operations.
"""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, fields
from functools import cache, partial
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING, Any

from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from pathlib import Path

DEFAULT = "message_ix_models.report.legacy.default_tables"


@dataclass
class Units:
    """Units.

    The fields exactly correspond to those found under ``model_units:`` in files like
    :file:`default_units.yaml`. Adding, removing, or changing keys without adjusting
    the class will result in errors.
    """

    conv_c2co2: float = 44.0 / 12.0
    conv_co22c: float = 12.0 / 44.0
    #: Carbon content of natural gas.
    crbcnt_gas: float = 0.482
    #: Carbon content of oil.
    crbcnt_oil: float = 0.631
    #: Carbon content of coal.
    crbcnt_coal: float = 0.814
    currency_unit_out: str = "US$2010"
    currency_unit_out_conv: float = 1.10774
    gwp_ch4: float = 25
    gwp_n2o: float = 298
    # comment appearing in :file:`default_units.yaml`:
    #
    #   HFC factors: GWP-HFC134a / HFC-Species
    #   GWP from Guus Velders (SSPs 2015 scen: OECD-SSP2)
    #   Email to Riahi, Fricko 20150713
    gwp_HFC125: float = 1360.0 / 3450.0
    gwp_HFC134a: float = 1360.0 / 1360.0
    gwp_HFC143a: float = 1360.0 / 5080.0
    gwp_HFC227ea: float = 1360.0 / 3140.0
    gwp_HFC23: float = 1360.0 / 12500.0
    gwp_HFC245fa: float = 1360.0 / 882.0
    gwp_HFC365: float = 1360.0 / 804.0
    gwp_HFC32: float = 1360.0 / 704.0
    gwp_HFC4310: float = 1360.0 / 1650.0
    gwp_HFC236fa: float = 1360.0 / 8060.0
    gwp_HFC152a: float = 1360.0 / 148.0

    @classmethod
    def from_dict(cls, data: dict) -> "Units":
        """Construct a Units instance from `data`, for instance from a file."""
        return cls(**{k: _maybe_eval(e) for (k, e) in data.items()})


@dataclass
class Config:
    """Configuration for :func:`.legacy.iamc_report_hackathon.main`.

    The fields unify the following, which overlap in some cases:

    1. Keys under the ``report_config:`` mapping in files like
       :file:`default_run_config.yaml.
    2. Arguments to :func:`.iamc_report_hackathon.main`.
    """

    #: Fully-qualified name of the module containing tables.
    table_def: str = DEFAULT

    #: Path to a CSV file.
    aggr_def: Path = Path("default_aggregates.csv")

    #: Path to a CSV file.
    var_def: Path = Path("default_variable_definitions.csv")

    #: Path to a YAML file.
    unit_yaml: Path = Path("default_units.yaml")

    #: Path to a CSV file.
    urban_perc: Path = Path("default_pop_urban_rural.csv")

    #: Only used when :attr:`run_history` is :any:`True`.
    kyoto_hist: Path = Path("default_kyoto_hist.csv")

    #: Only used when :attr:`run_history` is :any:`True`.
    lu_hist: Path = Path("default_lu_co2_hist.csv")

    merge_hist: bool = False
    merge_ts: bool = False
    run_history: bool = False

    #: Same as module global :py:`mu` in :mod:`.legacy.pp_utils` and others. See
    #: :func:`read_unit_config`.
    model_units: Units = field(default_factory=Units)

    #: Same as module global :py:`unit_conversion` in :mod:`.legacy.pp_utils` and
    #: others. See :func:`read_unit_config`. Nested :class:`dict` in which the first-
    #: and second-level keys are units to be converted from and to and the second-level
    #: values are conversion factors that multiply values expressed in the ‘from’ units.
    unit_conversion: dict[str, dict[str, float]] = field(default_factory=dict)

    #: List of :class:`.TableConfig` instances corresponding to the entries under
    #: ``run_tables:`` in files like :file:`default_run_config.yaml`.
    table: list["TableConfig"] = field(default_factory=list)

    def __post_init__(self) -> None:
        for f in filter(lambda f: f.type is Path, fields(self)):
            # Convert str (e.g. from YAML) to Path
            path = Path(getattr(self, f.name))
            # Resolve a non-absolute or non-existing path within the package data
            if not path.exists():
                path = package_data_path("report", "legacy", path)
                assert path.exists()
            setattr(self, f.name, path)

    @classmethod
    def from_file(cls, path) -> "Config":
        """Read a file like :file:`default_run_config.yaml`."""
        import yaml

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        cfg = cls(**data.pop("report_config"))

        # Name of module containing table functions
        module_name = cfg.table_def

        # Convert tables to instances of TableConfig
        for info in data.pop("run_tables").values():
            # Skip items without "active: True"
            if not info.pop("active", False):
                continue

            # Convert YAML contents to TableConfig instance
            try:
                tc = TableConfig.from_info(module_name=module_name, **info)
            except AttributeError:
                tc = TableConfig.from_info(module_name=DEFAULT, **info)
            cfg.table.append(tc)

        cfg.model_units, cfg.unit_conversion = read_unit_config(cfg.unit_yaml)

        return cfg


@dataclass(frozen=True)
class TableConfig:
    """Information needed by a legacy reporting table."""

    #: Python code snippet to be evaluated at runtime to determine whether
    #: :attr:`function` should run.
    condition: str

    #: Direct reference to a function from :mod:`.report.legacy.default_tables` or a
    #: similar module. This may be a :any:`functools.partial` object that fixes certain
    #: arguments to the function.
    function: Callable

    #: Reference to the module in which :attr:`function` appears.
    module: ModuleType

    #: Name of :func:`function` with the prefix "retr_" stripped.
    name: str

    #: Prefix for an IAMC ‘variable’ code.
    variable_prefix: str

    @classmethod
    def from_info(
        cls,
        module_name: str,
        *,
        args,
        condition: str | None = None,
        function: str,
        root,
    ) -> "TableConfig":
        """Construct from keys/values such as in :file:`default_run_config.yaml`."""
        mod = import_module(module_name)
        f = partial(getattr(mod, function), **args)
        name = function.partition("retr_")[2]

        return cls(condition or "", f, mod, name, root)

    def __repr__(self) -> str:
        return f"<TableConfig {self.module.__name__}.{self.name}>"


def _maybe_eval(expr: str, *args) -> Any:
    """Evaluate `expr`, or return as-is."""
    try:
        return eval(expr, *args)
    except Exception:
        return expr


@cache
def read_unit_config(path: "Path") -> tuple[Units, dict]:
    """Read a file like :file:`default_units.yaml`.

    Returns
    -------
    tuple
       …containing:

       1. :class:`Units` for :attr:`Config.model_units`.
       2. :class:`dict` for :attr:`Config.unit_conversion`.
    """
    import yaml

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    # Construct a Units instance. This validates the contents of the ``model_units:``
    # subtree
    model_units = Units.from_dict(data.pop("model_units"))

    # Globals expected by expressions to be eval'd() in ``conversion_factors:``
    globals = dict(mu=asdict(model_units))

    # Evaluate expressions in the second-level key *and* in the value
    conversion_factors: dict[str, dict[str, float]] = defaultdict(dict)
    for a, data_a in data.pop("conversion_factors").items():
        for b, expr in data_a.items():
            conversion_factors[a][_maybe_eval(b, globals)] = _maybe_eval(expr, globals)

    # File should contain only the above 2 keys
    assert not data

    return model_units, conversion_factors
