        slack_inputs = []
        for rule in SLACK_TECHNOLOGY_RULES:
            r = rule.copy()
            common_args = {
                "name": r["type"], 
                "technology": r["technology"], 
                "value": r["value"], 
                "unit": r["unit"], 
                "level": r["level"], 
                "commodity": r["commodity"], 
            }
            match rule["technology"]:
                case "return_flow"|"gw_recharge":
                    df_rule = make_df(
                        **common_args, 
                        mode=r["mode"], 
                        year_vtg=year_wat, 
                        year_act=year_wat, 
                    ).pipe(
                        broadcast, 
                        node_loc=df_node["node"], 
                        time=pd.Series(sub_time), 
                    ).pipe(same_node).pipe(same_time)
                    slack_inputs.append(df_rule)
                case "basin_to_reg":
                    df_rule = make_df(
                        **common_args, 
                        mode=eval(r["mode"], {}, {"df_node": df_node}), 
                        node_origin=eval(r["node_origin"], {}, {"df_node": df_node}), 
                        node_loc=eval(r["node_loc"], {}, {"df_node": df_node}), 
                    ).pipe(
                        broadcast, 
                        year_vtg=year_wat, 
                        time=pd.Series(sub_time), 
                    ).pipe(same_time)
                    slack_inputs.append(df_rule)

                case "salinewater_return":
                    #skip
                    pass
                case _:
                    raise ValueError(f"Invalid technology: {r['technology']}")
        slack_inputs = pd.concat(slack_inputs)
        slack_inputs["year_act"] = slack_inputs["year_vtg"]






        var_costs = []
        for rule in DUMMY_VARIABLE_COST_RULES:
            r = rule.copy()
            common_args = {
                "name": r["type"],
                "technology": r["technology"],
                "value": r["value"],
                "unit": r["unit"],

            }
            match r["technology"]:
                case "basin_to_reg":
                    common_args.update(
                        mode=eval(r["mode"], {}, {"df_node": df_node}),
                        node_loc=eval(r["node_loc"], {}, {"df_node": df_node}),
                    )
                    df_rule = make_df(**common_args).pipe(broadcast, year_vtg=year_wat, time=pd.Series(sub_time))
                    df_rule["year_act"] = df_rule["year_vtg"]
                    var_costs.append(df_rule)
                case "extract_surfacewater"|"extract_groundwater":
                    pass
                case _:
                    raise ValueError(f"Invalid technology: {r['technology']}")
        results["var_cost"] = pd.concat(var_costs)
