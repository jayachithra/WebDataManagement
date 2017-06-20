from cassandra.cluster import Cluster
cluster = Cluster()
cassandra = cluster.connect('wdm')
#cassandra.execute("INSERT INTO tbl_movies_by_id (id) values (1);")
#LIST OF NAMES AND IDS: SELECT idactors,fname,lname FROM actors WHERE idactors IN (SELECT idactors FROM acted_in WHERE idmovies = 11);

import psycopg2

conn = psycopg2.connect("dbname='IMDB_DB' user='postgres' host='localhost' password='postgres'")
postgres = conn.cursor()
row={}
query =""
item_amount=100
postgres.execute("SELECT * FROM genres;")
genres = postgres.fetchall()
for genre in genres:
	genre_id = genre[0]
	if genre_id>=29 or genre_id==29:
		continue
	genre_name = genre[1]
	print genre_name
	postgres.execute("SELECT MAX(year) FROM movies WHERE idmovies IN ( SELECT idmovies FROM movies_genres WHERE idgenres="+str(genre_id)+");")
	max_year=postgres.fetchall()[0][0]
	print(max_year)
	postgres.execute("SELECT MIN(year) FROM movies WHERE idmovies IN ( SELECT idmovies FROM movies_genres WHERE idgenres="+str(genre_id)+");")
	min_year=postgres.fetchall()[0][0]
	print(""+str(min_year)+" "+str(max_year))
	for i in range(max_year,max_year+1):
		print ""+str(i)+" with "+str(genre_id)
		postgres.execute("SELECT idmovies,title FROM movies WHERE year = "+str(i)+" AND idmovies IN (SELECT idmovies FROM movies_genres WHERE idgenres="+str(genre_id)+");")
		movies=postgres.fetchall()
		movies_id=[]
		movies_name=[]
		for movie in movies:
			movies_id.append(movie[0])
			movies_name.append(movie[1].replace("\'"," "))
		cassandra.execute("INSERT INTO tbl_genres_by_name_and_year (genre,year,movies_id,movies_title) values(\'"+genre_name.lower()+"\',"+str(i)+","+str(movies_id)+","+str(movies_name)+");")

print 'finished'
