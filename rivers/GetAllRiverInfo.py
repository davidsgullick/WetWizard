#!/usr/bin/env python2.7
# This script downloads information about all known rivers from the rainchasers
# api and stores them.

# Only use this to get all known info, get-ins, get-outs etc. A different script
# handles updating levels etc


""" Set up local environment """

import urllib
import json
import os.path as osp
import time
import sqlite3 as lite

#used to sanitze data for the db
import re

#string sanitization pattern
pattern = re.compile('([^\s\w\.,]|_)+')

#option parser allows us to have command line options!
from optparse import OptionParser


#object definitions:
class RiverInfo:
    """A class to hold each river data"""
    UUID = ""
    URL = ""
    SECTION = ""
    KM = ""
    GRADE = ""
    DIRECTIONS=""
    PUTIN_LAT = 0
    PUTIN_LON = 0
    TAKEOUT_LAT = 0
    TAKEOUT_LON = 0
    RIVER = ""
    DESCRIPTION = ""

    def __init__(self,id,url,section,km,grade,directions,putin_lat,putin_lon,takeout_lat,takeout_lon,river,description):
        self.UUID = id
        self.URL = url
        if section is None:
            section = ""
        self.SECTION = pattern.sub('', section)
        self.KM = km
        self.GRADE = grade
        if directions is None:
            directions = ""
        self.DIRECTIONS=pattern.sub('', directions)
        self.PUTIN_LAT = putin_lat
        self.PUTIN_LON = putin_lon
        self.TAKEOUT_LAT = takeout_lat
        self.TAKEOUT_LON = takeout_lon
        self.RIVER = river
        if description is None:
           description = ""
        self.DESCRIPTION = pattern.sub('', description)


    def generateSQLInsertStatement(self):
        s = "INSERT OR IGNORE INTO RiverInfo(UUID, URL, SECTION, KM, GRADE, DIRECTIONS, PUTIN_LAT, PUTIN_LON, TAKEOUT_LAT, TAKEOUT_LON, RIVER, DESCRIPTION) VALUES('{0}','{1}','{2}','{3}','{4}','{5}',{6},{7},{8},{9},'{10}','{11}')".format(self.UUID, self.URL, self.SECTION, self.KM, self.GRADE, self.DIRECTIONS, self.PUTIN_LAT, self.PUTIN_LON, self.TAKEOUT_LAT, self.TAKEOUT_LON, self.RIVER, self.DESCRIPTION)
      #  print s
        return s

class RiverSample:
    """A class that holds the sample data from each river"""
    River_UUID = ""
    TEXT = ""
    SOURCE_TYPE = ""
    SOURCE_NAME = ""
    SOURCE_VALUE = 0
    VALUE= 0
    TIMESTAMP=0

    def __init__(self, id, txt,stype,sname,svalue,value,timestamp):
        self.River_UUD=id
        self.TEXT= pattern.sub('', txt)
        self.SOURCE_TYPE=stype
        self.SOURCE_NAME=pattern.sub('', sname)
        self.SOURCE_VALUE=svalue
        self.VALUE=value
        self.TIMESTAMP=timestamp

    def generateSQLInsertStatement(self):
        s = "INSERT OR IGNORE INTO RiverSample(River_UUID, TEXT, SOURCE_TYPE, SOURCE_NAME, SOURCE_VALUE, VALUE, TIMESTAMP) VALUES('{0}','{1}','{2}','{3}',{4},{5},'{6}')".format(self.River_UUD , self.TEXT,self.SOURCE_TYPE, self.SOURCE_NAME, self.SOURCE_VALUE, self.VALUE, self.TIMESTAMP)
       # print s
        return s

#testRiverSample = RiverSample("testid", "testtext","type", "sname", "svalue","value","timestamp")
#print testRiverSample.generateSQLInsertStatement()

#testRiverInfo = RiverInfo("id","url","section","km","grade","directions","putin_lat","putin_lon","takeout_lat","takeout_lon","river","description")
#print testRiverInfo.generateSQLInsertStatement()

#exit()

#this is the database connection. Used if needed.
con = None

#new parser object
parser = OptionParser()


parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True,  help="don't print status messages to stdout. Default is verbose.")
parser.add_option("--usesql", action="store_true",  dest="usedatabase", default=False,  help="Save to specified sqllite database instead of txt format. Default is txt.")
parser.add_option("--sqllocation", action="store", type="string", dest="databasename", default=osp.join(osp.expanduser("~"), "WetWizard", "data") + "/sqldatabase.sqlite",  help="location of database. Default is ~/WetWizard/sqldatabase.sqlite. --usesql is required for this to take effect.")

#get the options from the parser.
(options, args) = parser.parse_args()

#print options.databasename



def connectToSQLiteDB( dbname ):
    "Connects to a sqlite database, creating it if it doesnt exist."
    global con
    #first we need to see if the database exists.
    con = lite.connect(dbname)
    with con:
        try:
            cur = con.cursor()    
            cur.execute('CREATE TABLE IF NOT EXISTS "main"."RiverInfo" ("UUID" VARCHAR PRIMARY KEY  NOT NULL , "URL" VARCHAR NOT NULL , "SECTION" VARCHAR, "KM" VARCHAR NOT NULL  DEFAULT 0, "GRADE" VARCHAR NOT NULL  DEFAULT 0, "DIRECTIONS" VARCHAR, "PUTIN_LAT" DOUBLE NOT NULL , "PUTIN_LON" DOUBLE NOT NULL , "TAKEOUT_LAT" DOUBLE NOT NULL , "TAKEOUT_LON" DOUBLE NOT NULL , "RIVER" VARCHAR NOT NULL , "DESCRIPTION" VARCHAR, UNIQUE (UUID))');
            cur.execute('CREATE TABLE IF NOT EXISTS "RiverSample" ("SampleID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL  , "River_UUID" VARCHAR , "TEXT" VARCHAR, "SOURCE_TYPE" VARCHAR, "SOURCE_NAME" VARCHAR, "SOURCE_VALUE" DOUBLE, "VALUE" DOUBLE, "TIMESTAMP" REAL, UNIQUE (River_UUID, TIMESTAMP)) ');
        except:
            pass



def addRiverSample(sampleObj):
    "this adds a sample to the database, to the correct river."
    global con
    if not con:
        con = lite.connect(dbname)
    with con:
        try:
            cur = con.cursor() 
            cur.execute(sampleObj.generateSQLInsertStatement());
        except:
            print "Failed on " + sampleObj.generateSQLInsertStatement()
            pass

def addRiverDescription(riverDesc):
    "this adds a sample to the database, to the correct river."
    global con
    if not con:
        con = lite.connect(dbname)
    with con:
        try:
            cur = con.cursor() 
            cur.execute(riverDesc.generateSQLInsertStatement());
        except:
            print "Failed on " + riverDesc.generateSQLInsertStatement()
            pass

def closeDB():
    "this disconnects from the database"
    global con
    if con:
         con.close() 




def doEncode(s):
    if isinstance(s, basestring):
       return  s.encode('utf-8')
    else:
        return str(s);

home = osp.expanduser("~")
data_dir = osp.join(home, "WetWizard", "data")


""" Now we're ready to download the data """

rivers = []
next_river = 'http://api.rainchasers.com/v1/river' # default unless we are just updating. in that case the link is stored here: ResumeLink.txt


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
    #print(len(rivers))
    try:
        next_river = chunk['meta']['link']['next']
    except KeyError:
        next_river = chunk['meta']['link']['resume']
        break

with open(osp.join(data_dir, "ResumeLink.txt"), "w") as output:
    output.write(next_river)

with open(osp.join(data_dir, "CurrentRiverData.txt"), "w") as output:
    json.dump(rivers, output)

if options.usedatabase:

    #connect to the database
    connectToSQLiteDB(options.databasename)
    

    for x in rivers:

        r = RiverInfo(x['uuid'],x['url'],x['section'],x['km'],x['grade']['text'], x['directions'],x['position'][0]['lat'],x['position'][0]['lng'],x['position'][1]['lat'],x['position'][1]['lng'],x['river'],x['desc'])
        addRiverDescription(r)
        if 'state' in x:
            #id, txt,stype,sname,svalue,value,timestamp
            sample = RiverSample(x['uuid'], x['state']['text'], x['state']['source']['type'], x['state']['source']['name'], x['state']['source']['value'], x['state']['value'], x['state']['time'])
            addRiverSample(sample)
           # print sample.generateSQLInsertStatement()
        




        #print r.generateSQLInsertStatement()


#        print x['section']
    #    print x['km']
   #     print x['grade']['text']
   #     print x['grade']['value']

        #it seems that some do not have a state.
   #     if 'state' in x:
   #         print x['state']['text']
   #         print x['state']['value']
    #        print x['state']['time']
    #        print x['state']['source']['type']
    #        print x['state']['source']['name']
    #        print x['state']['source']['value']

     #   print x['directions']
     #   print x['position'][0] #type, lat lng   
     #   print x['position'][1]
     #   print x['river']
     #   print x['desc']

#[KEY]state:[VALUE]{u'text': u'empty', u'source': {u'type': u'ea', u'name': u'Cwmystwyth', u'value': u'0.23'}, u'value': -0.01, u'time': 1433721600}

#[KEY]position:[VALUE][{u'lat': 52.3483983, u'lng': -3.7794502, u'type': u'putin'}, {u'lat': 52.3384714, u'lng': -3.84867, u'type': u'takeout'}]

    #disconnect from the database
    closeDB()
