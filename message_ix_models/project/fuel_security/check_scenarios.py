import ixmp
import message_ix

mp = ixmp.Platform()

slist = mp.scenario_list()

slist = slist[slist['model'] == 'SSP_SSP2_v6.6']
print(slist['scenario'].unique().tolist())

mp.close_db()