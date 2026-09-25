import ixmp
import message_ix

mp = ixmp.Platform()

slist = mp.scenario_list()
slist = slist[slist['model'] == 'SSP_SSP2_v6.6']
slist = slist[slist['scenario'] == 'INDC2030i_forever']
print(slist)

mp.close_db()