# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 12:30:42 2017
Script to run query on a database and store the results in json 
@author: Jaya
"""


import json
hostname = 'localhost'
username = 'postgres'
password = '******'
database = 'movie'


def doQuery( conn ) :
 
    cur = conn.cursor()
	
    movieid = []
    actors = []
    series = []
    genres = []
    keywords = []
    jsondata = []
    movieid = list(movieid)
    mtitle, myear, mtype = [],[],[]
    
    #READ movies from movies table
    cur.execute("SELECT idmovies, title, year, type FROM movies where idmovies between 1 and 100 ")
    
    for idmovies, title, year, mvtype in cur.fetchall():
        movieid.append(idmovies)
        mtitle.append(title)
        myear.append(year)
        mtype.append(mvtype)
        
    
    #For each movie get the list of movies he/she has acted in
    for x in movieid:
        #print (x)
        nameofseries = []
        actorsin = []
        genrenames = []
        keywordnames = []
       
        #----------name of series..............
        cur.execute("select s.name from series s where s.idmovies = (%s)",(x,))
        for sername in cur.fetchall():
            nameofseries.append(str(sername).strip().split('\'')[1])
        series.append(nameofseries)
        
        #-----------------name of genres----------
        cur.execute("select  g.genre from genres g join movies_genres m on g.idgenres=m.idgenres where m.idmovies = (%s)",(x,))
        for genname in cur.fetchall():
            genrenames.append(str(genname).strip().split('\'')[1])
        genres.append(genrenames)
        
        #------------------all keywords-------------
        cur.execute("select k.keyword from keywords k join movies_keywords m on k.idkeywords=m.idkeywords where m.idmovies = (%s)",(x,))
        for keyname in cur.fetchall():
            keywordnames.append(str(keyname).strip().split('\'')[1])
        keywords.append(keywordnames)    

        cur.execute("select a.fname || ' ' || a.lname || '('  || ai.character || ')' from actors a join acted_in ai on a.idactors = ai.idactors join movies m on ai.idmovies=m.idmovies where m.idmovies = (%s) group by a.idactors, ai.character, ai.billing_position order by ai.billing_position",(x,))
        for aname in cur.fetchall():
           
            if(str(aname)== "(None,)"):
                continue
            print(str(aname).strip().split('\'')[1])
            actorsin.append( str(aname).strip().split('\'')[1])
            
        actors.append(actorsin)
    #add the actor details and movie details to a single list- jsondata
   
    for x in range(0,len(actors)):
       jsondata.append({"movieid": movieid[x], "title": mtitle[x], "year": myear[x], "type": mtype[x], "series": series[x], "genres": genres[x], "keywords": keywords[x], "actors" : actors[x]})
      
        
    
        
    #append the final list to a json file
    with open('moviedata.json', 'w') as f:
     json.dump(jsondata, f)
     for x in jsondata:
         f.write("{}\n".format(json.dumps(x)))

print ("Using psycopg2…")
import psycopg2
myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
doQuery( myConnection )
myConnection.close()


