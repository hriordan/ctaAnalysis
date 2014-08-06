import sqlite3 
import csv
from StringIO import StringIO
import operator
from pandas import Series, DataFrame
import numpy


rawdata = open("CTA_info.csv", "r")

list_data = list(rawdata)

del list_data[0]


for i,entry in enumerate(list_data):
	data = StringIO(entry)
	reader = csv.reader(data, delimiter=",")
	for row in reader:
		list_data[i] = tuple(row)


query = """ 
	CREATE TABLE stop_info 
	(
		id INT PRIMARY KEY,
		on_street varchar(30),
		cross_street varchar(30),
		routes varchar(50), 
		boardings int,
		alightings int,
		month_beginning DATETIME,
		daytpe varchar(10),
		location varchar(50)
	);
	"""
stop_info_keys = ['id,','on_street','cross_street','routes',
	'boardings','alightings','month_beginning','daytpe','location']

con = sqlite3.connect(':memory:')
con.execute(query)
con.commit()

stmt = "INSERT INTO stop_info VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
con.executemany(stmt, list_data)
con.commit()


query = """
		CREATE TABLE stop_route_pair 
		(
			stop_id INT, 
			route_id varchar(50)
		);
	"""

con.execute(query)
con.commit()

routelist = []
for entry in list_data:
	routes = entry[3].split(',')
	for route in routes: 
		stmt = "INSERT INTO stop_route_pair VALUES(?, ?)"  #
		con.execute(stmt,(entry[0], str(route)))
		con.commit()
		if route not in routelist:
			routelist.append(route)

routedict = {}

for route in routelist: 

	cursor = con.execute('SELECT COUNT(stop_id) AS stopCounts FROM stop_route_pair WHERE route_id == ?' ,
				(route,))
	rows = cursor.fetchall()
	routedict[route] = rows[0][0]
	

sorted_routes = sorted(routedict.iteritems(), key=operator.itemgetter(1))


#display top 10 longest routes
top10 = [route[0] for route in sorted_routes[-10:]]
top10i = [route[1] for route in sorted_routes[-10:]]

	
route_lengths = Series(top10, top10i)
route_lengths.name = "route"
route_lengths.index.name = "stop-count"
print route_lengths

raw_input("continue?")


lengths = [route[1] for route in sorted_routes]
routeAvgLength = numpy.mean(lengths)
routeMedLength = numpy.median(lengths)
print Series([routeAvgLength, routeMedLength], ["AvgRouteLength","MedRouteLength"])

raw_input("Continue?")

stmt = """ 
		SELECT stop_id, MAX(occurence) FROM (  
			SELECT stop_id, COUNT(*) AS occurence FROM  	
				(SELECT * FROM stop_route_pair) a group by stop_id)  ; 
		"""

cursor = con.execute(stmt)
rows = cursor.fetchall()
print "Most visited stop is stop %s, with %d lines visiting it" % (rows[0][0],rows[0][1])

raw_input("Continue?")

stmt = """ SELECT * FROM stop_info WHERE id = ?; """

cursor = con.execute(stmt, (rows[0][0],) ) 
rows = cursor.fetchall()

most_visited_stop = Series(list(rows[0]), stop_info_keys, name='stop_info')

print "\nMore information about this stop:"
print most_visited_stop

raw_input("Continue?")

#Let's find out about Michigan Avenue's ridership!
stmt = """SELECT AVG(boardings) as AvgBoardings, SUM(boardings) as TotalBoardings 
	FROM stop_info WHERE on_street = ?;"""

cursor = con.execute(stmt, ("MICHIGAN",)) 
rows = cursor.fetchall()
print "\nMichigan Ave's average boarding count is %f and has a total of %f boardings on all Michigan stops" % (rows[0][0], rows[0][1])

raw_input("Continue?")

#graphs
boardstmt = """SELECT boardings, alightings, id FROM stop_info 
					WHERE on_street = ?;"""
					
cursor = con.execute(boardstmt, ("MICHIGAN",)) 
rows = cursor.fetchall()
michBoards = [i[0] for i in rows]
michAlights = [i[1] for i in rows]
michIDs = [i[2] for i in rows]

data = {'boardings': michBoards, 'alightings': michAlights, 'stop_id': michIDs}
michdf = DataFrame(data)


boardplt = plt.scatter(range(1, len(michBoards)+1), michdf['boardings'])
alightplt = plt.scatter(range(1, len(michBoards)+1), michdf['alightings'], c='r')

plt.legend((boardplt, alightplt), ("boardings", "alightings"))
plt.show()
raw_input("Continue?")
plt.close()

boardstmt = """SELECT stop_id FROM stop_route_pair WHERE route_id = ?;"""
cursor = con.execute(boardstmt, ("9",)) 
rows = cursor.fetchall()


raw_input()

stmt = """SELECT * FROM stop_info WHERE id = ?;"""
results = []
for i in rows:
	cursor.execute(stmt, i )
	results.append( cursor.fetchall() )

topBoards = [i[0][4] for i in results]
topAlights = [i[0][5] for i in results]
topIDs = [i[0][0] for i in results]

topboardplt = plt.scatter(range(1, len(topBoards)+1), topBoards)
topalightplt = plt.scatter(range(1, len(topBoards)+1), topAlights, c = 'r')
plt.legend((boardplt, alightplt), ("boardings", "alightings"))

plt.show()
