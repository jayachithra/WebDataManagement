# -*- coding: utf-8 -*-
"""
Created on Fri May 19 19:56:20 2017

@author: Jaya
"""

"""
Load json database into your local couchdb database
or change the db string to point you any other db on network
* use at your own risk and feel free to use and distribute this code * 
Author Tanmay Dutta

"""
import sys
import couchdb
import json

DB_STRING="http://jaya:12345@127.0.0.1:5984"


def main(json_file,
         db_name,
         couchdb_address):
    # 1. Create the database.. No error checking here because we want to get exception if db exists
    couch = couchdb.Server(couchdb_address)
    db = couch[db_name]
    #db = couch.create(db_name)
    print(db)
    # 2 Read the json line by line and put into the db
    with open(json_file) as jsonfile:
        for row in jsonfile:
            db_entry = json.loads(row) 
            db.save(db_entry)
    


if __name__=='__main__':
    print ("Call as <jsondb.json> <new_db> <optional db string>")
    #args = sys.argv[1:]
    #json_file = args[0]
    ##try:
      #  db_name = args[1]
    #except:
      #  db_name = json_file.split(".")[0]
    #try:
     #   db_str = args[2]
    #except:
    db_str = DB_STRING
    main('I:/movieinfo.json',
         'imdb',
         db_str)