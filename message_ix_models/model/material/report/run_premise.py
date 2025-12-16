if __name__ == "__main__":
    import premise
    from message_ix import Reporter

    from message_ix_models import Context
    from message_ix_models.report import prepare_reporter

    ctx = Context()
    import ixmp
    import message_ix

    med_and_medlow = [
        ("SSP_SSP2_v6.5", "baseline_DEFAULT_step_14"),  # fmy = 2020
        ("SSP_SSP2_v6.5", "NPi2030"),  # fmy = 2030
        ("SSP_SSP2_v6.5", "NPiREF"),  # fmy = 2035; Medium Emissions is clone
        ("SSP_SSP2_v6.5", "NPiREF_SSP2 - Medium-Low Emissionsf"),  # fmy = 2045
    ]
    low_emi = [
        ("SSP_SSP2_v6.5", "INDC2030i_weak"),  # fmy = 2030
        ("SSP_SSP2_v6.5", "INDC2030i_weak_SSP2 - Low Emissions"),  # fmy = 2035
    ]
    high = [
        ("SSP_SSP3_v6.5", "baseline_DEFAULT_step_14"),  # fmy = 2020
        ("SSP_SSP3_v6.5", "baseline"),  # fmy = 2030; High Emissions is clone
        ("SSP_SSP5_v6.5", "baseline"),  # fmy = 2030; High Emissions is clone
    ]
    scens = med_and_medlow + low_emi + high
    mp = ixmp.Platform("ixmp_dev")
    for model_name, scen_name in scens:
        print(f"Running premise for {model_name} - {scen_name}")
        scen = message_ix.Scenario(mp, model_name, scen_name)
        rep = Reporter.from_scenario(scen)
        prepare_reporter(ctx, reporter=rep)
        df = premise.run(rep, scen, scen.model, scen.scenario)
        df = df.filter(unit=["dimensionless", "", None], keep=False).rename(
            mapping={"unit": {"Mt / a": "Mt/yr", "EJ / a": "EJ/yr"}}
        )
        df.to_excel(f"{model_name}_{scen_name}.xlsx")
        del scen
    print()
