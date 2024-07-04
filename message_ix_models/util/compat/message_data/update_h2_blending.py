from message_ix_models import ScenarioInfo
from message_ix_models.util import nodes_ex_world


def main(scen):
    """Revise hydrogen-blending constraints.

    The revision of the constraints makes changes to three relations.
    1. For relation `h2_scrub_limit`, two CCS technologies are removed.
       The relation should limit hydrogen share via blending in all non-CCS gas
       applications to 50%, hence `h2_mix` has an entry of
       -.482tC/kWyr*2*input-coefficient, while all other technologies have an
       entry of .482tC/kWyr*input-coefficient. Greenfield CCS technologies do
       not require an entry into the relation, as well as any other
       technologies which require the "C" for the output e.g. `meth_ng` or
       `h2_smr`.
       A negative entry from the greenfield-CCS technologies would further
       reduce the "total" gas. Theortically, `gas_coal` should also have an
       entry, but is not really necessary as this technology is only active in
       BAU scenarios, when this constraint is not active. `bio_gas` has a
       negative entry to ensure that it doesnt count towards the total used to
       derive the share. The retro-fit CCS scrubbers require an entry, because
       the gas used in powerplants to which the retrofits apply, can be used
       for CCS pruposes and this needs to be avoided.
       Only secondary-level technologies need to be added.
    2. The relation `h2mix_direct` is removed in favor of #3. This avoids
       having to add individual technologies at the final-energy level.
    3. The relation `gas_mix_lim` is used to limit the share of hydrogen
       blended into the the natural-gas network based on total final-energy
       gas use. Previously, this constraint was only configured for AFR
       (at 20%). The constraint is carried over to other regions and set at
       50% in all regions including AFR.
    """

    # ----------------------------------
    # Step 1.: Clean up `h2_scrub_limit`
    # ----------------------------------

    with scen.transact("Clean up h2_scrub_limit entries"):
        df = scen.par(
            "relation_activity",
            filters={
                "relation": "h2_scrub_limit",
                "technology": ["gas_cc_ccs", "h2_smr_ccs"],
            },
        )
        scen.remove_par("relation_activity", df)

    # ----------------------------------------------------------
    # Step 2.: Reconfigure hydrogen blending based on FE-Gas use
    # ----------------------------------------------------------
    with scen.transact("Reconfigure hydrogen-mixing constraints"):
        # Remove obsolete relation
        scen.remove_set("relation", "h2mix_direct")

        # Extend and update values of `gas_mix_lim`
        rel_act = scen.par("relation_activity", filters={"relation": "gas_mix_lim"})
        rel_act.loc[rel_act.technology.isin(["gas_t_d", "gas_t_d_ch4"]), "value"] = -0.5

        # Copy lower_bound
        rel_bnd = scen.par("relation_upper", filters={"relation": "gas_mix_lim"})

        # Update parameters for all regions except GLB and World.
        for n in nodes_ex_world(ScenarioInfo(scen).N):
            rel_act = rel_act.assign(node_rel=n, node_loc=n)
            scen.add_par("relation_activity", rel_act)

            rel_bnd = rel_bnd.assign(node_rel=n)
            scen.add_par("relation_upper", rel_bnd)
