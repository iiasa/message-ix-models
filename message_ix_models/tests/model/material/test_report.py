from itertools import product

import pandas as pd
import pyam

from message_ix_models.model.material.report.reporting import convert_mass_to_energy

_t = ["CH2O_synth", "MTO_petro"]
_ltm = list(
    product(
        ["primary", "primary_material"],
        ["bio", "bio_ccs", "coal", "coal_ccs", "h2", "ng", "ng_ccs"],
        ["feedstock", "fuel"],
    )
)
#: List of variables expected by convert_mass_to_energy()
VARIABLE = [f"in|final_material|methanol|{t}|M1" for t in _t] + [
    f"out|{l_}|methanol|meth_{t}|{m}" for l_, t, m in _ltm
]


def test_convert_mass_to_energy() -> None:
    df = pd.DataFrame([[v, 1.0] for v in VARIABLE], columns=["variable", 1]).assign(
        model="m", scenario="s", region="r", unit="u"
    )
    idf = pyam.IamDataFrame(df)

    # Function runs without error
    result = convert_mass_to_energy(idf)

    # TODO Add assertions
    del result
