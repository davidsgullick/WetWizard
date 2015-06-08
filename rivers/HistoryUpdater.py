#!/usr/bin/env python2.7
# Run under python3
# This script updates a dictionary of historical data from any new data

import os.path as osp
import json
from robust_json import JsonLoad

home = osp.expanduser("~")
data_dir = osp.join(home, "WetWizard", "data")


""" Now we update historic data of all rivers and levels """

rivers = JsonLoad(osp.join(data_dir, "NewRiverData.txt"))
levels_dict = JsonLoad(osp.join(data_dir, "RiverLevels.txt"))

for entry in rivers:
    try:
        river = entry['river']
        section = entry['section']
        time = entry['state']['time']
        level = entry['state']['source']['value']
    except KeyError:
        continue

    if river in levels_dict:
        if section in levels_dict[river]:
            if time not in levels_dict[river][section]:
                levels_dict[river][section][time] = level
        else:
            levels_dict[river][section] = {time: level}
    else:
        levels_dict[river] = {section: {time: level}}


with open(osp.join(data_dir, "RiverLevels.txt"), "w") as f:
    json.dump(levels_dict, f)
