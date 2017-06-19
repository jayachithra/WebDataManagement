# -*- coding: utf-8 -*-
"""
Created on Fri Jun  9 14:15:48 2017
Script that runs a query on actors and genres tables and stores the result in a json format.
@author: Jaya
"""
import json
hostname = 'localhost'
username = 'postgres'
password = '******'
database = 'movie'

def doQuery( conn ) :
    #connect to Postgres  
    cur = conn.cursor()
    
	
    movies = []
    jsondata = []
    mgenre, mgenreid = [], []
    myear= []
    
    tyear, tgenre = [], []
	
    #READ actors from actor table
    cur.execute("SELECT distinct m.year from movies m ")
    
    for year in cur.fetchall():
        myear.append(year)
        
    print(myear)
    
    #READ genres from genre table
    cur.execute("SELECT distinct g.genre, g.idgenres from genres g ")
    
    for genre, genreid in cur.fetchall():
        mgenre.append(genre)
        mgenreid.append(genreid)
        
   
    length = len(myear) * len(mgenre)
    print(mgenre)
    print(mgenreid)
    i = 0
    #For each actor get the list of movies he/she has acted in
    for x in myear:
     if(i==0):
          i = 1
     else:
      x=str(x).strip().split('(')[1]      
      x=str(x).strip().split(',')[0]
     
         
      for y in mgenre:
        
        tyear.append(x)
        tgenre.append(y)
        
        moviesin = []
        cur.execute("Select m.title from movies m join movies_genres mg on m.idmovies = mg.idmovies join genres g on mg.idgenres=g.idgenres where g.idgenres = (select ge.idgenres from genres ge where ge.genre = %s) and m.year = %s group by m.title, m.year, g.genre order by m.year",(y,x) )
        for title in cur.fetchall():
            
            #moviesin list has all movies by a particular actor
            moviesin.append( str(title).strip().split('\'')[1])
        #append the list for each actor into the main list 
        movies.append(moviesin)
        
    
    #add the actor details and movie details to a single list- jsondata
    length = (len(myear)-1) * len(mgenre)
    print(length)
    for x in range(0,length):
               jsondata.append({"year": int(tyear[x]), "genre": tgenre[x], "movies": movies[x],"number_of_movies" : len(movies[x])})
        
    
    with open('genres.json', 'w') as f:
     
     for x in jsondata:
         f.write("{}\n".format(json.dumps(x)))

print ("Using psycopg2…")
import psycopg2
myConnection = psycopg2.connect( host=hostname, user=username, password=password, dbname=database )
doQuery( myConnection )
myConnection.close()


