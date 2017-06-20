from cassandra.cluster import Cluster
cluster = Cluster()
cassandra = cluster.connect('wdm')
#cassandra.execute("INSERT INTO tbl_movies_by_id (id) values (1);")
#LIST OF NAMES AND IDS: SELECT idactors,fname,lname FROM actors WHERE idactors IN (SELECT idactors FROM acted_in WHERE idmovies = 11);

import psycopg2

maxid=1356171

columns = ["id", "title", "year", "number", "type", "location", "language"]

conn = psycopg2.connect("dbname='IMDB_DB' user='postgres' host='localhost' password='postgres'")
postgres = conn.cursor()
row={}
query =""
item_amount=100
for i in xrange(1062800,maxid+1,item_amount):
	print ""+str(i)+" out of "+str(maxid)
#	try:
	postgres.execute("SELECT * FROM movies WHERE idmovies>="+str(i)+" AND idmovies<"+str(i+item_amount)+";")
	rows = postgres.fetchall()
	for row in rows:
		postgres.execute("SELECT idactors,fname,lname FROM actors WHERE idactors IN (SELECT idactors FROM acted_in WHERE idmovies = "+str(row[0])+");");
		actors_list = postgres.fetchall()
		actors_id=""
		actors_name=""
		if len(actors_list)<=0:
			actors_id="[]"
			actors_name="[]"
		else:
			actors_id = "["+str(actors_list[0][0])+""
			actors_name=""
			if actors_list[0][2] is None and actors_list[0][1] is None:
				actors_name+= "[\'N/A\'"
			elif actors_list[0][2] is None:
				actors_name+= "[\'"+actors_list[0][1].replace("\'"," ")+"\'"
			elif actors_list[0][1] is None:
				actors_name+= "[\'"+actors_list[0][2].replace("\'"," ")+"\'"
			else:
				actors_name+= "[\'"+actors_list[0][1].replace("\'"," ")+" "+actors_list[0][2].replace("\'"," ")+"\'"
			for j in range(1,len(actors_list)-1):
				actors_id+=","+str(actors_list[j][0])+""
				if actors_list[j][2] is None and actors_list[j][1] is None:
					actors_name+= ",\'N/A\'"
				elif actors_list[j][2] is None:
					actors_name+= ",\'"+actors_list[j][1].replace("\'"," ")+"\'"
				elif actors_list[j][1] is None:
					actors_name+= ",\'"+actors_list[j][2].replace("\'"," ")+"\'"
				else:
					actors_name+= ",\'"+actors_list[j][1].replace("\'"," ")+" "+actors_list[j][2].replace("\'"," ")+"\'"
			actors_id+="]"
			actors_name+="]"
		values = str(row[0])
		column = columns[0]
		for j in range(1,len(columns)-1):
			if str(row[j])!="None":
				column+=","+columns[j]
				if j==2 or j==3:
					values+=","+str(row[j]).replace("\'"," ")
				else:
					values+=",\'"+str(row[j]).replace("\'"," ")+"\'"
		query = "INSERT INTO tbl_movies_by_id ("+column+",actors_id,actors_name"+") values("+values+","+actors_id+","+actors_name+")"
		cassandra.execute(query)
#	except Exception,e: 
#		print query
#		print str(e)

print 'finished'
