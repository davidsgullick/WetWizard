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

#sqlite db
import sqlite3 as lite
#option parser allows us to have command line options!
from optparse import OptionParser

#object definitions:
class RiverInfo:
    """A class to hold each river's meta-data, but not level info. """
    UUID = ""
    URL = ""
    SECTION = ""
    KM = ""
    GRADE = ""
    DIRECTIONS=""
    PUTIN_LAT = -1
    PUTIN_LON = -1
    TAKEOUT_LAT = -1
    TAKEOUT_LON = -1
    RIVER = ""
    DESCRIPTION = ""

    def __init__(self,id,url,section,km,grade,directions,putin_lat,putin_lon,takeout_lat,takeout_lon,river,description):
        #first we sanitize the input variables. We check they are not None. Those that are are set to some default value. (Blank or -1)
        id = "Not Set" if id is None else id
        url = "Not Set" if url is None else url
        section = "Not Set" if section is None else section
        km = "Not Set" if km is None else km
        grade = "Not Set" if grade is None else grade
        directions = "Not Set" if directions is None else directions
        putin_lat = -1 if putin_lat is None else putin_lat
        putin_lon = -1 if putin_lon is None else putin_lon
        takeout_lat = -1 if takeout_lat is None else takeout_lat
        takeout_lon = -1 if takeout_lon is None else takeout_lon
        river = "Not Set" if river is None else river
        description = "Not Set" if description is None else description

        #now we assign the variables.
        self.UUID = id
        self.URL = url
        self.SECTION = section
        self.KM = km
        self.GRADE = grade
        self.DIRECTIONS= directions
        self.PUTIN_LAT = putin_lat
        self.PUTIN_LON = putin_lon
        self.TAKEOUT_LAT = takeout_lat
        self.TAKEOUT_LON = takeout_lon
        self.RIVER = river
        self.DESCRIPTION = description


    def insertIntoDatabase(self, connection):
        with con:
            try:
                cur = con.cursor() 
                #execute this as a prepared statement into sql. helps with quoting & escaping of strange characters. Additional protection against SQL injection.
                cur.execute("INSERT OR IGNORE INTO RiverInfo(UUID, URL, SECTION, KM, GRADE, DIRECTIONS, PUTIN_LAT, PUTIN_LON, TAKEOUT_LAT, TAKEOUT_LON, RIVER, DESCRIPTION) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (self.UUID, self.URL, self.SECTION, self.KM, self.GRADE, self.DIRECTIONS, self.PUTIN_LAT, self.PUTIN_LON, self.TAKEOUT_LAT, self.TAKEOUT_LON, self.RIVER, self.DESCRIPTION));
            except lite.Error as er:
                print 'Failed on:', er.__dict__

class RiverSample:
    """A class that holds the sample data from each river"""
    River_UUID = ""
    TEXT = ""
    SOURCE_TYPE = ""
    SOURCE_NAME = ""
    SOURCE_VALUE = -1
    VALUE= -1
    TIMESTAMP=-1

    def __init__(self, id, txt,stype,sname,svalue,value,timestamp):
        #first we sanitize the input variables. We check they are not None. Those that are are set to some default value. (Blank or -1)
        id = "Not Set" if id is None else id
        txt = "Not Set" if txt is None else txt
        stype = "Not Set" if stype is None else stype
        sname = "Not Set" if sname is None else sname
        svalue = -1 if svalue is None else svalue
        value = -1 if value is None else value
        timestamp = "0" if timestamp is None else timestamp
        #Now we assign the variables.
        self.River_UUD=id
        self.TEXT= txt
        self.SOURCE_TYPE=stype
        self.SOURCE_NAME=sname
        self.SOURCE_VALUE=svalue
        self.VALUE=value
        self.TIMESTAMP=timestamp

    def insertIntoDatabase(self, connection):
        with con:
            try:
                cur = con.cursor() 
                #execute this as a prepared statement into sql. helps with quoting & escaping of strange characters. Additional protection against SQL injection.
                cur.execute("INSERT OR IGNORE INTO RiverSample(River_UUID, TEXT, SOURCE_TYPE, SOURCE_NAME, SOURCE_VALUE, VALUE, TIMESTAMP) VALUES(?,?,?,?,?,?,?)",(self.River_UUD , self.TEXT,self.SOURCE_TYPE, self.SOURCE_NAME, self.SOURCE_VALUE, self.VALUE, self.TIMESTAMP));
            except lite.Error as er:
                print 'Failed on:', er.__dict__



#this is the database connection. Used if needed.
con = None

#new parser object
parser = OptionParser()
#quiet?
parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True,  help="don't print status messages to stdout. Default is verbose.")
#are we using sql
parser.add_option("-s","--usesql", action="store_true",  dest="usedatabase", default=False,  help="Save to specified sqllite database instead of txt format. Default is txt.")
#are we using txt
parser.add_option("-t","--usetxt", action="store_true",  dest="usetxt", default=False,  help="Not implemented Fully.Should we store the data as a text base format?.")
#what is sqllocation directory
parser.add_option("--sqllocation", action="store", type="string", dest="databasename", default=osp.join(osp.expanduser("~"), "WetWizard", "data") + "/sqldatabase.sqlite",  help="location of database. Default is ~/WetWizard/sqldatabase.sqlite. --usesql is required for this to take effect.")
#switch to enable the user to force a full download rather than update (which is default when available)
parser.add_option("-f","--performFullDownload", action="store_true", dest="fullDownload", default=False ,  help="By default we try to limit what we download by only downloading new data. This option forces the redownload of everything.")
#Allow import of a stored txt base dict file into the database
parser.add_option("--import", action="store", type="string", dest="importfile", default="",  help="NOT FULLY IMPLIMENTED.Allows the user to import a txt stored dictionary of data into the database. Only makes sense with the --usesql flag...")
#Allow the user to purge the database
parser.add_option("-p","--purge", action="store_true", dest="doPurge", default=False ,  help="NOT FULLY IMPLIMENTED.**WARNING** this erases all data from the database!**")




#get the options from the parser.
(options, args) = parser.parse_args()


#Here we do some sanity checking. E.g we must be using atleast --usesql or --usetxt
if options.usedatabase is False and options.usetxt is False and options.doPurge is False:
    print "You have not selected a storage method. Please use either (or both of) --usesql flag for sqlite database or --usetxt glag for txt file dictionarys."
    exit(1)
if(options.usetxt is True and options.usedatabase is False and options.importfile is not ""):
    print "You have tried to import a txt base dictionary file into another txt based dictionary file. This option is only supported with sqlite storage."
    exit(1)
if(options.doPurge is True):
    print ""
    response = raw_input("**WARNING!** This will delete all collected data in the database, and purge all relevant txt files. \nPlease consider backups. To continue, please type 'DELETE', and then press the enter key:\n>")
    if(response == "DELETE"):
        print "Purging database..."
        #TODO: need to actually purge the database. Not wise to keep this here whilst testing..
        print "Done"
        exit(0)
    else:
        print "Purge aborted, your entered text did not match 'DELETE'."
        exit(1)


def connectToSQLiteDB( dbname ):
    "Connects to a sqlite database, creating it if it doesnt exist."
    global con
    con = lite.connect(dbname)
    with con:
        try:
            cur = con.cursor()    
            cur.execute('CREATE TABLE IF NOT EXISTS "main"."RiverInfo" ("UUID" NVARCHAR PRIMARY KEY  NOT NULL , "URL" NVARCHAR NOT NULL , "SECTION" NVARCHAR, "KM" NVARCHAR NOT NULL  DEFAULT 0, "GRADE" NVARCHAR NOT NULL  DEFAULT 0, "DIRECTIONS" NVARCHAR, "PUTIN_LAT" DOUBLE NOT NULL , "PUTIN_LON" DOUBLE NOT NULL , "TAKEOUT_LAT" DOUBLE NOT NULL , "TAKEOUT_LON" DOUBLE NOT NULL , "RIVER" NVARCHAR NOT NULL , "DESCRIPTION" NVARCHAR, UNIQUE (UUID))');
            cur.execute('CREATE TABLE IF NOT EXISTS "RiverSample" ("SampleID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL  , "River_UUID" NVARCHAR , "TEXT" NVARCHAR, "SOURCE_TYPE" NVARCHAR, "SOURCE_NAME" NVARCHAR, "SOURCE_VALUE" DOUBLE, "VALUE" DOUBLE, "TIMESTAMP" REAL, UNIQUE (River_UUID, TIMESTAMP)) ');
            cur.execute('CREATE  TABLE IF NOT EXISTS "main"."MetaInfo" ("VarName" NVARCHAR PRIMARY KEY  NOT NULL  UNIQUE )')
            cur.execute('CREATE TABLE IF NOT EXISTS "MetaInfo" ("VarName" NVARCHAR PRIMARY KEY  NOT NULL  UNIQUE, "VarValue" NVARCHAR )')
            cur.execute("INSERT INTO MetaInfo VALUES('NextLink','http://api.rainchasers.com/v1/river')")
        except:
            pass


def closeDB():
    "this disconnects from the database"
    global con
    if con:
         con.close() 


def purge(): #TODO:need to make this!
    pass

def getUpdateLinkFromDB():
    #SELECT VarValue From MetaInfo Where VarName = "NextLink"
    global con
    with con:
        try:
            cur = con.cursor()   
            cur.execute("SELECT VarValue From MetaInfo Where VarName = 'NextLink'")
            row = cur.fetchone()
            if row == None:
                return 'http://api.rainchasers.com/v1/river'
            else:
                return row[0]
        except lite.Error as er:
            print 'Failed on:', er.__dict__

def updateLinkInDB(newLink):
    global con
    with con:
        try:
            cur = con.cursor() 
            #execute this as a prepared statement into sql. helps with quoting & escaping of strange characters. Additional protection against SQL injection.
            cur.execute("UPDATE MetaInfo SET VarValue = ? Where VarName = 'NextLink'", (newLink,));
        except lite.Error as er:
            print 'Failed on:', er.__dict__


rivers = []
resumeLink= ""
def populateRiverDictionary(url):
    global rivers
    global resumeLink
    next_river = url #'http://api.rainchasers.com/v1/river'

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
        try:
            next_river = chunk['meta']['link']['next']
        except KeyError:
            resumeLink = chunk['meta']['link']['resume']
            break




#set up home directory stuff
home = osp.expanduser("~")
data_dir = osp.join(home, "WetWizard", "data")







#Are we updating a database too?
if options.usedatabase:
    #connect to the database
    connectToSQLiteDB(options.databasename)

    #Now we're ready to download the data. This updates the list and sets a variable in the database
    #tht remembers where we got up to.
    resumeLink = getUpdateLinkFromDB()
    populateRiverDictionary(resumeLink)
    updateLinkInDB(resumeLink)

    #for each river
    for x in rivers:
        #create a new riverinfo object
        r = RiverInfo(x['uuid'],x['url'],x['section'],x['km'],x['grade']['text'], x['directions'],x['position'][0]['lat'],x['position'][0]['lng'],x['position'][1]['lat'],x['position'][1]['lng'],x['river'],x['desc'])
        #put it into database. (It will be ignored fail if it exists already!)
        r.insertIntoDatabase(con);

        #if we have a measurement, make a sample object and put that in the database too.
        if 'state' in x:
            sample = RiverSample(x['uuid'], x['state']['text'], x['state']['source']['type'], x['state']['source']['name'], x['state']['source']['value'], x['state']['value'], x['state']['time'])
            sample.insertIntoDatabase(con)
     
     #close up!
    closeDB()
