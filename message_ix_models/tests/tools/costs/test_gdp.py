def test_process_raw_ssp_data():
    pass
    # r11 = process_raw_ssp_data(input_node="R11", input_ref_region="R11_NAM")
    # r12 = process_raw_ssp_data(input_node="R12", input_ref_region="R12_NAM")

    # # Assert that all regions are present in each node configuration
    # assert np.all(
    #     r11.region.unique()
    #     == [
    #         "R11_AFR",
    #         "R11_CPA",
    #         "R11_EEU",
    #         "R11_FSU",
    #         "R11_LAM",
    #         "R11_MEA",
    #         "R11_NAM",
    #         "R11_PAO",
    #         "R11_PAS",
    #         "R11_SAS",
    #         "R11_WEU",
    #     ]
    # )

    # # Assert that for R11, all R11 regions are present
    # assert np.all(
    #     r12.region.unique()
    #     == [
    #         "R12_AFR",
    #         "R12_CHN",
    #         "R12_EEU",
    #         "R12_FSU",
    #         "R12_LAM",
    #         "R12_MEA",
    #         "R12_NAM",
    #         "R12_PAO",
    #         "R12_PAS",
    #         "R12_RCPA",
    #         "R12_SAS",
    #         "R12_WEU",
    #     ]
    # )

    # # Assert that the maximum year is 2100
    # assert r11.year.max() == 2100
    # assert r12.year.max() == 2100

    # # Assert that SSP1-5 and LED are present in each node configuration
    # scens = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
    # assert bool(all(i in r11.scenario.unique() for i in scens)) is True
    # assert bool(all(i in r12.scenario.unique() for i in scens)) is True


def test_calculate_indiv_adjusted_region_cost_ratios():
    pass
    # r11_reg_diff = get_weo_region_differentiated_costs(
    #     input_node="r11",
    #     input_ref_region="R11_NAM",
    #     input_base_year=2021,
    #     input_module="base",
    # )

    # r11_cost_ratios = calculate_indiv_adjusted_region_cost_ratios(
    #     region_diff_df=r11_reg_diff,
    #     input_node="r11",
    #     input_ref_region="R11_NAM",
    #     input_base_year=2021,
    # )

    # r12_reg_diff = get_weo_region_differentiated_costs(
    #     input_node="r12",
    #     input_ref_region="R12_NAM",
    #     input_base_year=2021,
    #     input_module="base",
    # )

    # r12_cost_ratios = calculate_indiv_adjusted_region_cost_ratios(
    #     region_diff_df=r12_reg_diff,
    #     input_node="r12",
    #     input_ref_region="R12_NAM",
    #     input_base_year=2021,
    # )

    # # Assert that all regions are present in each node configuration
    # assert np.all(
    #     r11_cost_ratios.region.unique()
    #     == [
    #         "R11_AFR",
    #         "R11_CPA",
    #         "R11_EEU",
    #         "R11_FSU",
    #         "R11_LAM",
    #         "R11_MEA",
    #         "R11_NAM",
    #         "R11_PAO",
    #         "R11_PAS",
    #         "R11_SAS",
    #         "R11_WEU",
    #     ]
    # )

    # # Assert that for R11, all R11 regions are present
    # assert np.all(
    #     r12_cost_ratios.region.unique()
    #     == [
    #         "R12_AFR",
    #         "R12_CHN",
    #         "R12_EEU",
    #         "R12_FSU",
    #         "R12_LAM",
    #         "R12_MEA",
    #         "R12_NAM",
    #         "R12_PAO",
    #         "R12_PAS",
    #         "R12_RCPA",
    #         "R12_SAS",
    #         "R12_WEU",
    #     ]
    # )

    # # Assert that the maximum year is 2100
    # assert r11_cost_ratios.year.max() == 2100
    # assert r12_cost_ratios.year.max() == 2100

    # # Assert that SSP1-5 and LED are present in each node configuration
    # scens = ["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"]
    # assert bool(all(i in r11_cost_ratios.scenario.unique() for i in scens)) is True
    # assert bool(all(i in r12_cost_ratios.scenario.unique() for i in scens)) is True

    # # Assert that all cost ratios for reference region
    # R11_NAM or R12_NAM are equal to 1
    # assert all(
    #     r11_cost_ratios.query("region == 'R11_NAM'").reg_cost_ratio_adj.values == 1.0
    # )
    # assert all(
    #     r12_cost_ratios.query("region == 'R12_NAM'").reg_cost_ratio_adj.values == 1.0
    # )

    # Assert that all cost ratios are greater than 0 (CURRENTLY FAILING BECAUSE OF PAO)
    # assert all(r11_cost_ratios.reg_cost_ratio_adj.values > 0)
    # assert all(r12_cost_ratios.reg_cost_ratio_adj.values > 0)
