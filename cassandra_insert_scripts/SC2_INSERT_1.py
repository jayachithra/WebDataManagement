from cassandra.cluster import Cluster
cluster = Cluster()
cassandra = cluster.connect('wdm')
#cassandra.execute("INSERT INTO tbl_movies_by_id (id) values (1);")
#LIST OF NAMES AND IDS: SELECT idactors,fname,lname FROM actors WHERE idactors IN (SELECT idactors FROM acted_in WHERE idmovies = 11);

import psycopg2

maxid=2361865

columns = ["id" , "fname" , "lname" , "mname" , "gender" , "number" , "aka" ]

conn = psycopg2.connect("dbname='IMDB_DB' user='postgres' host='localhost' password='postgres'")
postgres = conn.cursor()
row={}
query =""
item_amount=100
for i in xrange(0,maxid+1,item_amount):
	print ""+str(i)+" out of "+str(maxid)
#	try:
	postgres.execute("SELECT * FROM actors WHERE idactors>="+str(i)+" AND idactors<"+str(i+item_amount)+";")
	rows = postgres.fetchall()
	for row in rows:
		movies_id = "[]"
		movies_name="[]"
		postgres.execute("SELECT idmovies,title FROM movies WHERE idmovies IN (SELECT idmovies FROM acted_in WHERE idactors = "+str(row[0])+");");
		movies_list = postgres.fetchall()
		if len(movies_list)>0:
			movies_id = "["+str(movies_list[0][0])+""
			movies_name= "[\'"+movies_list[0][1].replace("\'"," ")+"\'"
			for j in range(1,len(movies_list)-1):
				movies_id+=","+str(movies_list[j][0])+""
				movies_name+= ",\'"+movies_list[j][1].replace("\'"," ")+"\'"
			movies_id+="]"
			movies_name+="]"
			values = str(row[0])
			column = columns[0]
			for j in range(1,len(columns)-1):
				if str(row[j])!="None":
					column+=","+columns[j]
					if j==0 or j==5:
						values+=","+str(row[j]).replace("\'"," ")
					else:
						values+=",\'"+str(row[j]).replace("\'"," ").replace("\""," ")+"\'"
		postgres.execute("SELECT name FROM aka_names WHERE idactors="+str(row[0])+";")
		aka_list = postgres.fetchall()
		aka_list_write = []
		for i in range(0,len(aka_list)):
			aka_list_write.append(aka_list[i][0].replace("\'"," "))
		if len(aka_list)==0:
			query = "INSERT INTO tbl_actors_by_id ("+column+",movies_id,movies_title"+") values("+values+","+movies_id+","+movies_name+")"
		else:
			query = "INSERT INTO tbl_actors_by_id ("+column+",aka,movies_id,movies_title"+") values("+values+","+str(aka_list_write)+","+movies_id+","+movies_name+")"
		cassandra.execute(query)
#	except Exception,e: 
#		print query
#		print str(e)

print 'finished'
