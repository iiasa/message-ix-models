import ixmp
import message_ix

mp = ixmp.Platform()

scenarios = ['NPi2030_bilateralize', 'NPi2030_FSU2100']

for s in scenarios:

    scenario = message_ix.Scenario(mp, model='fuel-security', scenario=s)

    # European gas imports
    eur_gas = scenario.var('ACT')
    eur_gas = eur_gas[(eur_gas['technology'].str.contains('_exp_weu'))|(eur_gas['technology'].str.contains('_exp_eeu'))]
    eur_gas = eur_gas[eur_gas['lvl']>0]

    eur_gas = eur_gas.groupby(['year_act', 'node_loc', 'technology'])['lvl'].sum().reset_index()

    print("--------------------------------")
    print(s)
    print("--------------------------------")
    print("2030")
    print("--------------------------------")
    eur_gas_2030 = eur_gas[eur_gas['year_act'] == 2030]
    print(eur_gas_2030)

    print("--------------------------------")
    print("2060")
    print("--------------------------------")
    eur_gas_2060 = eur_gas[eur_gas['year_act'] == 2060]
    print(eur_gas_2060)

mp.close_db()