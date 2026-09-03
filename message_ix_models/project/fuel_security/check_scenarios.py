import ixmp
import message_ix

mp = ixmp.Platform()

slist = mp.scenario_list()
slist = slist[(slist['cre_user'] == 'shepard') & (slist['model'] == 'fuel_security')]
print(slist)

mp.close_db()