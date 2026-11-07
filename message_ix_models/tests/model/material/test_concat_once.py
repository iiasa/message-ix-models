"""Tests that :mod:`.model.material` concatenates once after a loop."""

import ast
from ast import literal_eval
from pathlib import Path

import pandas as pd
import pytest
from message_ix import make_df

import message_ix_models.model.material
from message_ix_models.model.material.data_methanol import (
    broadcast_nodes,
    broadcast_years,
    unpivot_input_data,
)
from message_ix_models.util import package_data_path

#: Loops that read the accumulator inside the loop body, so it cannot be collected
#: into a list. Keys are (path relative to model/material, function, target).
LOOP_CARRIED = {
    ("report/config.py", "use_aggregates_dict", "self.mapping"),
    ("report/run_reporting.py", "split_fe_other", "py_df_all"),
    ("report/run_reporting.py", "split_mto_feedstock", "py_df_all"),
}


def _unpivot_input_data_reference(df: pd.DataFrame, par_name: str) -> pd.DataFrame:
    """Copy of :func:`.unpivot_input_data` with one concat per iteration."""
    df_final = df
    df_final_full = pd.DataFrame()

    for i in df_final.index:
        node_cols = [i for i in df_final.columns if "node" in i]
        remove = ["'", "[", "]", " "]
        node_cols_codes = {}
        for col in node_cols:
            node_cols_codes[col] = pd.Series(
                "".join(x for x in df_final.loc[i][col] if x not in remove).split(",")
            )

        df_bc_node = make_df(par_name, **df_final.loc[i])

        yr_cols_codes = {}
        yr_col_inp = [i for i in df_final.columns if "year" in i]
        yr_col_out = [i for i in df_bc_node.columns if "year" in i]
        df_bc_node[yr_col_inp] = df_final.loc[i][yr_col_inp].values

        for colname in node_cols:
            df_bc_node[colname] = None
        df_bc_node = broadcast_nodes(
            df_bc_node, df_final, node_cols, node_cols_codes, i
        )

        for col in yr_col_inp:
            yr_cols_codes[col] = literal_eval(df_bc_node[col].values[0])
            df_bc_node = broadcast_years(df_bc_node, yr_col_out, yr_cols_codes, col)
        df_bc_node[yr_col_out] = df_bc_node[yr_col_out].astype(int)

        df_final_full = pd.concat([df_final_full, df_bc_node])
    df_final_full = df_final_full.drop_duplicates().reset_index(drop=True)

    if par_name == "relation_activity":
        df_final_full = df_final_full.drop(
            df_final_full[
                (df_final_full.node_rel.values != "R12_GLB")
                & (df_final_full.node_rel.values != df_final_full.node_loc.values)
            ].index
        )
    return make_df(par_name, **df_final_full)


@pytest.fixture(scope="module")
def methanol_pars() -> dict[str, pd.DataFrame]:
    return pd.read_excel(
        package_data_path("material", "methanol", "methanol_techno_economic.xlsx"),
        sheet_name=["input", "inv_cost", "relation_activity", "emission_factor"],
        dtype=object,
    )


@pytest.mark.parametrize(
    "par_name", ["input", "inv_cost", "relation_activity", "emission_factor"]
)
@pytest.mark.parametrize("n_rows", [1, 6])
def test_unpivot_input_data(methanol_pars, par_name, n_rows) -> None:
    df = methanol_pars[par_name].head(n_rows)

    exp = _unpivot_input_data_reference(df, par_name)
    obs = unpivot_input_data(df, par_name)

    assert len(obs)
    pd.testing.assert_frame_equal(obs, exp, check_exact=True)


@pytest.mark.parametrize("par_name", ["input", "inv_cost"])
def test_unpivot_input_data_empty(methanol_pars, par_name) -> None:
    df = methanol_pars[par_name].iloc[:0]

    exp = _unpivot_input_data_reference(df, par_name)
    obs = unpivot_input_data(df, par_name)

    pd.testing.assert_frame_equal(obs, exp, check_exact=True)


def _concat_in_loop(path: Path) -> list[tuple[str, str, int]]:
    """Return (function, target, line) for ``X = *.concat([..., X, ...])`` in loops."""
    result = []

    def visit(node: ast.AST, func: str, in_loop: bool) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func, in_loop = node.name, False
        elif isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            in_loop = True

        if (
            in_loop
            and isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Attribute)
            and node.value.func.attr == "concat"
            and node.value.args
            and isinstance(node.value.args[0], (ast.List, ast.Tuple))
        ):
            target = ast.unparse(node.targets[0])
            if any(ast.unparse(e) == target for e in node.value.args[0].elts):
                result.append((func, target, node.lineno))

        for child in ast.iter_child_nodes(node):
            visit(child, func, in_loop)

    visit(ast.parse(path.read_text()), "", False)
    return result


def test_no_concat_accumulator_in_loop() -> None:
    root = Path(message_ix_models.model.material.__file__).parent
    found = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        for func, target, line in _concat_in_loop(path):
            if (rel, func, target) not in LOOP_CARRIED:
                found.append(f"{rel}:{line} {func}(): {target}")

    assert not found, (
        "Collect frames in a list and concat once after the loop:\n" + "\n".join(found)
    )
