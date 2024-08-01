from message_ix_models.model.transport import factor


class TestFactor:
    def test_quantify(self):
        # Example structural info
        years = [2020, 2025, 2030, 2050, 2100, 2110]
        nodes = ["R12_AFR", "R12_NAM", "R12_WEU"]
        technology = ["ELC_100", "HFC_ptrp", "PHEV_ptrp"]

        layers = [
            factor.Map(
                "setting",
                L=factor.Constant(0.8, "n y t"),
                M=factor.Constant(1.0, "n y t"),
                H=factor.Constant(1.2, "n y t"),
            ),
            factor.Omit(y=[2020]),
            factor.Keep(t=["ELC_100", "PHEV_ptrp"]),
        ]

        f = factor.Factor(layers)

        # quantify() method runs
        result = f.quantify(y=years, n=nodes, t=technology)

        # print(
        #     result.to_series()
        #     .reset_index()
        #     .sort_values(by=["setting", "n", "t", "y"])
        #     .to_string()
        # )

        assert {"n", "t", "y", "setting"} == set(result.dims)

        # Now using the ScenarioSetting layer
        f.layers.append(factor.ScenarioSetting(SSP1="L", SSP2="M", SSP3="H"))

        # quantify() method runs
        result = f.quantify(y=years, n=nodes, t=technology, scenario="SSP1")

        # print(
        #     result.to_series().reset_index().sort_values(by=["n", "t", "y"])
        #     .to_string()
        # )

        assert {"n", "t", "y"} == set(result.dims)
