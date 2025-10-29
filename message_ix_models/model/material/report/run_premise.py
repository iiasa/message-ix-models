if __name__ == "__main__":
    import premise
    from message_ix import Reporter

    from message_ix_models import Context
    from message_ix_models.report import prepare_reporter

    ctx = Context()
    import ixmp
    import message_ix

    mp = ixmp.Platform("local3")
    scen = message_ix.Scenario(mp, "SSP_SSP2_v6.2", "baseline_wo_GLOBIOM_ts")
    rep = Reporter.from_scenario(scen)
    prepare_reporter(ctx, reporter=rep)
    df = premise.run(rep, scen, scen.model, scen.scenario)
    print()
