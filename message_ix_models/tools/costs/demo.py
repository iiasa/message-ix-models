from message_ix_models.tools.costs.projections import create_cost_inputs

create_cost_inputs("inv_cost", ssp_scenario="ssp3", format="message")
create_cost_inputs("fix_cost", ssp_scenario="ssp1", format="iamc")


# TODO:
# - create code to upload to model scenario in database connection
