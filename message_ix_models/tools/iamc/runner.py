"""IAMC community diagnostic assessment protocol scenario runner.

Implements Table 1 scenarios from:
    "IAM community diagnostic assessment protocol", version 1.0
    doi: 10.5281/zenodo.19554965

All scenario dispatch is driven by the CHARACTERIZATION flags and price_schedule
entries in :mod:`message_ix_models.tools.iamc.structure`. No scenario names are
hardcoded — adding a new entry to STATIC with the right characterization flags
makes it work automatically.

Dependencies: message_ix, ixmp — no message_data imports.
"""

from typing import TYPE_CHECKING, Callable

import message_ix
import pandas as pd
from ixmp.backend import ItemType

from message_ix_models.tools.add_bounded_relation import main as add_bounded_relation
from message_ix_models.tools.remove_emission_bounds import main as remove_emission_bounds
from message_ix_models.tools.reserve_margin import update as update_reserve_margin
from message_ix_models.tools.iamc.structure import (
    CHARACTERIZATION,
    PRICE_START_YR,
    PROTOCOL_CONV,
    STATIC,
    protocol_price_co2,
)

if TYPE_CHECKING:
    from message_ix_models import Context

_CO2_TO_C = 12.0 / 44.0
EJ_TO_GWYR = 31.71
GTCO2_TO_MTC = _CO2_TO_C * 1000

# Module-level lookup: base scenario id → STATIC entry
_STATIC_BY_ID: dict = {entry["id"]: entry for entry in STATIC}

C = CHARACTERIZATION


class ScenarioRunner:
    """Run IAMC community diagnostic assessment protocol scenarios.

    All dispatch is driven by :data:`~message_ix_models.tools.iamc.structure.STATIC`
    and :class:`~message_ix_models.tools.iamc.structure.CHARACTERIZATION` flags.

    Parameters
    ----------
    context : message_ix_models.Context
        Must have ``project_config["create_diagnostic"]`` with keys:

        - ``base_scenario`` : str — scenario to clone from
        - ``cp_scenario`` : str — CP (current policies) reference (default ``"CP"``)
        - ``firstmodelyear`` : int — first model year after clone (default 2025)
        - ``reporting`` : bool
        - ``dest_scen`` : dict mapping scenario name → config dict with:

          - ``active`` : bool
          - ``solve`` : bool
          - ``post_solve`` : callable ``(scen, context) -> None``, optional —
            invoked after solve (e.g. reporting). Replaces hard ``report()`` call.
          - ``additional_policies`` : list of callables ``(scen) -> None`` —
            used for :attr:`CHARACTERIZATION.additional_mitigation_policies`.
    """

    def __init__(self, context: "Context"):
        self.context = context
        self.mp = context.get_platform()
        self.output_model = context.scenario_info["model"]
        self.run_reporting_only = context.run_reporting_only

    # ------------------------------------------------------------------ #
    # Carbon price                                                         #
    # ------------------------------------------------------------------ #

    def _set_carbon_price(self, base_id: str, cp_scen: message_ix.Scenario) -> None:
        """Set tax_emission per protocol Table 2.

        For t < :data:`PRICE_START_YR`: retain CP price.
        For t >= :data:`PRICE_START_YR`: ``max(cp_price, protocol_price)``.
        Price path is read from the ``price_schedule`` lambda in STATIC.
        """
        years = list(self.scen.set("year"))
        df_cp = cp_scen.par(
            "tax_emission", {"node": "World", "type_emission": "TCE"}
        )
        cp_prices = df_cp.set_index("type_year")["value"].to_dict()

        rows = []
        for yr in years:
            cp_val = cp_prices.get(yr, 0.0)
            proto_val = protocol_price_co2(base_id, yr) * PROTOCOL_CONV
            val = cp_val if yr < PRICE_START_YR else max(cp_val, proto_val)
            rows.append({
                "node": "World",
                "type_emission": "TCE",
                "type_tec": "all",
                "type_year": yr,
                "value": val,
                "unit": "USD/tC",
            })

        df_new = pd.DataFrame(rows)
        df_existing = self.scen.par(
            "tax_emission", {"node": "World", "type_emission": "TCE"}
        )
        with self.scen.transact(f"Set {base_id} protocol carbon price"):
            if not df_existing.empty:
                self.scen.remove_par("tax_emission", df_existing)
            self.scen.add_par("tax_emission", df_new)

    def _remove_all_carbon_price(self) -> None:
        """Remove all tax_emission (No_Policy_baseline)."""
        df = self.scen.par("tax_emission")
        if not df.empty:
            with self.scen.transact("Remove all emission taxes (No-Policy)"):
                self.scen.remove_par("tax_emission", df)

    # ------------------------------------------------------------------ #
    # Constraints (global only, per protocol)                             #
    # ------------------------------------------------------------------ #

    def _apply_lim_bio(self) -> None:
        """Global primary bioenergy limit: 100 EJ/yr (Limiting_biomass)."""
        add_bounded_relation(
            self.scen,
            val=100 * EJ_TO_GWYR,
            tec=["land_use_biomass"],
            rel="limit_bioenergy_global",
            node="all",
        )

    def _apply_lim_ccs(self) -> None:
        """Global CCS limit: 2 GtCO₂/yr (Limiting_CCS)."""
        add_bounded_relation(
            self.scen,
            val=2,
            tec=["bco2_tr_dis", "co2_tr_dis"],
            rel="limit_ccs_global",
            val_conv=GTCO2_TO_MTC,
            val_unit="MtC/yr",
            node="all",
        )

    # ------------------------------------------------------------------ #
    # Infrastructure                                                       #
    # ------------------------------------------------------------------ #

    def _load_scenario(self, scenario: str, model: str | None = None) -> message_ix.Scenario:
        model = model or self.output_model
        return message_ix.Scenario(self.mp, model, scenario, cache=True)

    def _write_gdx(self) -> None:
        self.context._mp._backend.write_file(
            path=self.context.project_path
            / "tmp"
            / f"MsgData_{self.scen.model}_{self.scen.scenario}.gdx",
            item_type=ItemType.SET | ItemType.PAR,
            filters={"scenario": self.scen},
        )

    def solve(self, post_solve: Callable | None = None) -> None:
        """Solve and optionally call *post_solve* callback.

        Parameters
        ----------
        post_solve :
            Callable ``(scen, context) -> None`` invoked after solve, e.g. for
            reporting. Pass ``None`` (default) to skip.
        """
        if not self.run_reporting_only:
            update_reserve_margin(self.scen)
            self.scen.set_as_default()
            self.scen = self._load_scenario(self.scen.scenario)
            print(f"- Solving '{self.scen.scenario}' (MESSAGE-MACRO) ...")
            self.scen.solve(
                model="MESSAGE-MACRO",
                var_list=["I", "C", "GDP"],
                max_adjustment=0.2,
            )
        if post_solve is not None:
            post_solve(self.scen, self.context)

    # ------------------------------------------------------------------ #
    # Main entry point                                                     #
    # ------------------------------------------------------------------ #

    def run(self, scenarios: str) -> None:
        """Run IAMC diagnostic protocol scenarios.

        Dispatch is driven entirely by :data:`STATIC` entries.
        Each scenario is processed by reading its ``characterization`` flags:

        - :attr:`~CHARACTERIZATION.Current_policies` → retain CP prices (no-op)
        - :attr:`~CHARACTERIZATION.No_Policy_baseline` → remove all emission taxes
        - Any other scenario with a ``price_schedule`` → ``max(CP, protocol)``
        - :attr:`~CHARACTERIZATION.Limiting_biomass` → global 100 EJ bioenergy limit
        - :attr:`~CHARACTERIZATION.Limiting_CCS` → global 2 GtCO₂/yr CCS limit
        - :attr:`~CHARACTERIZATION.Limiting_CDR` → bioenergy + CCS limits stacked
        - :attr:`~CHARACTERIZATION.additional_mitigation_policies` → callbacks from config

        Parameters
        ----------
        scenarios :
            Must be ``"create_diagnostic"``.
        """
        if scenarios != "create_diagnostic":
            raise ValueError(f"Unknown scenario set: {scenarios!r}")

        cfg = self.context.project_config["create_diagnostic"]
        base_scen_nm = cfg["base_scenario"]
        cp_scen_nm = cfg.get("cp_scenario", "CP")
        firstyr = cfg.get("firstmodelyear", 2025)

        self.base = self._load_scenario(base_scen_nm)
        cp_scen = self._load_scenario(cp_scen_nm)

        for scen_nm, scen_cfg in cfg["dest_scen"].items():
            if not scen_cfg.get("active", True):
                continue

            base_id = scen_nm.split("-SSP")[0]
            if base_id not in _STATIC_BY_ID:
                raise ValueError(
                    f"Scenario '{scen_nm}' (base id '{base_id}') not in STATIC. "
                    f"Register it in message_ix_models/tools/iamc/structure.py. "
                    f"Valid ids: {list(_STATIC_BY_ID)}"
                )

            entry = _STATIC_BY_ID[base_id]
            chars = set(entry["characterization"])

            print(f"\n=== Building {scen_nm} ===")

            self.scen = self.base.clone(
                self.output_model,
                scen_nm,
                keep_solution=False,
                shift_first_model_year=firstyr,
            )
            self.scen.set_as_default()

            remove_emission_bounds(self.scen, remove_all=True)

            # ---- Carbon price (characterization-driven) ------------------ #
            if C.No_Policy_baseline in chars:
                self._remove_all_carbon_price()
            elif C.Current_policies in chars:
                pass  # retain CP prices as cloned
            elif entry.get("price_schedule") is not None:
                self._set_carbon_price(base_id, cp_scen)

            # ---- Constraints (characterization-driven) ------------------- #
            if C.Limiting_biomass in chars or C.Limiting_CDR in chars:
                self._apply_lim_bio()

            if C.Limiting_CCS in chars or C.Limiting_CDR in chars:
                self._apply_lim_ccs()

            # C400-lin-LimCDR extra limits (350 Mha afforestation, 0.5 GtCO₂/yr
            # biochar, 0 GtCO₂/yr ocean algae, 0.5 GtCO₂/yr enhanced weathering)
            # are land-use model specific — pass via additional_policies callbacks.

            if C.additional_mitigation_policies in chars:
                for policy_fn in scen_cfg.get("additional_policies", []):
                    policy_fn(self.scen)

            # ---- Solve or write GDX ------------------------------------- #
            if scen_cfg.get("solve", False):
                self.context.scenario_info["scenario"] = scen_nm
                self.solve(post_solve=scen_cfg.get("post_solve"))
            else:
                self._write_gdx()

            print(f"Done: {scen_nm}")
