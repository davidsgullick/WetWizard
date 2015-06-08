#!/usr/bin/env python2.7
# This grabs new river info


""" Set up local environment """

import urllib
import json
import os.path as osp
import time

home = osp.expanduser("~")
data_dir = osp.join(home, "WetWizard", "data")


""" Now we're ready to download the data """

rivers = []
with open(osp.join(data_dir, "ResumeLink.txt"), "r") as f:
    next_river = f.read()


while True:
    attempts = 0
    response = None

    while attempts < 3:
        try:
            response = urllib.urlopen(next_river)
            break
        except IOError:
            attempts += 1
            time.sleep(5)
    if not response:
        raise IOError("resync fail at " + next_river)

    chunk = json.loads(response.read())
    if chunk['status'] != 200:
        raise IOError("resync fail at " + next_river)
    rivers += chunk['data']
    print(len(rivers))
    try:
        next_river = chunk['meta']['link']['next']
    except KeyError:
        next_river = chunk['meta']['link']['resume']
        break

with open(osp.join(data_dir, "ResumeLink.txt"), "w") as output:
    output.write(next_river)

with open(osp.join(data_dir, "NewRiverData.txt"), "w") as output:
    json.dump(rivers, output)
