import json


# Load the json object if it is there, else return an empty dict
def JsonLoad(filename):
    try:
        with open(filename, "r") as f:
            new_list = json.load(f)
    except (IOError, ValueError):
        return {}
    return new_list
