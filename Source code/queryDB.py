from mysqlConn import DbConnect
import csv
import argparse

db = DbConnect()
db_conn = db.get_connection()
cur2 = db_conn.cursor();

#Argument parser: Give path of all csv files.
parser = argparse.ArgumentParser()
parser.add_argument("PATH")
args = parser.parse_args()

csv_path = args.PATH

#Get a path variable.

#import data to mysql db from the csv datafiles
print "Importing data from path : ", csv_path

cur2.execute('CREATE TABLE `imdb-actor-info`(actorid varchar(10) NOT NULL, name varchar(200) NOT NULL, gender varchar(2) NOT NULL)')
csv_data = csv.reader(file(csv_path+'/imdb-actor-info.csv'))
next(csv_data)
row_count=0;
for row in csv_data:
    cur2.execute('INSERT INTO `imdb-actor-info`(actorid, name, gender) VALUES(%s, %s, %s)',row)
    row_count+=1
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "imdb-actor-info - done"


cur2.execute('CREATE TABLE `genome-tags`(tagID varchar(4) NOT NULL, tag varchar(200) NOT NULL)')
csv_data = csv.reader(file(csv_path+'/genome-tags.csv'))
next(csv_data)
row_count=0;
for row in csv_data:
    cur2.execute('INSERT INTO `genome-tags`(tagID, tag) VALUES(%s, %s)',row)
    row_count+=1
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "genome-tags - done"



cur2.execute('CREATE TABLE `mlmovies`(movieid varchar(10) NOT NULL, moviename varchar(300) NOT NULL, year varchar(5) NOT NULL, genres varchar(200) NOT NULL)')
csv_data = csv.reader(file(csv_path+'/mlmovies.csv'))
next(csv_data)
row_count=0
for row in csv_data:
    cur2.execute('INSERT INTO `mlmovies`(movieid, moviename, year, genres) VALUES(%s, %s, %s, %s)',row)
    row_count+=1
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "mlmovies - done"

cur2.execute('CREATE TABLE `mlratings`(movieid varchar(10) NOT NULL, userid varchar(10) NOT NULL, imdbid varchar(10) NOT NULL, rating varchar(10) NOT NULL, timestamp varchar(50) NOT NULL)')
csv_data = csv.reader(file(csv_path+'/mlratings.csv'))
next(csv_data)
row_count=0
for row in csv_data:
    cur2.execute('INSERT INTO `mlratings`(movieid, userid, imdbid, rating, timestamp) VALUES(%s, %s, %s, %s, %s)',row)
    row_count+=1
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "mlratings done"

cur2.execute('CREATE TABLE `mltags`(userid varchar(20) NOT NULL , movieid varchar(20) NOT NULL, tagid varchar(10) NOT NULL, timestamp varchar(50) NOT NULL)')
csv_data = csv.reader(file(csv_path+'/mltags.csv'))
next(csv_data)
row_count=0
for row in csv_data:
    cur2.execute('INSERT INTO `mltags`(userid, movieid, tagid, timestamp) VALUES(%s, %s, %s, %s)',row)
    row_count+=1
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "mltags done"

cur2.execute('CREATE TABLE `mlusers`(userid varchar(10) NOT NULL)')
csv_data = csv.reader(file(csv_path+'/mlusers.csv'))
next(csv_data)
row_count=0
for row in csv_data:
    cur2.execute('INSERT INTO `mlusers`(userid) VALUES(%s)',row)
    row_count+=1
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "mlusers done"

cur2.execute('Create table `movie-actor`(movieid varchar(10) NOT NULL, actorid varchar(10) NOT NULL, actor_movie_rank int(10) NOT NULL)')
csv_data = csv.reader(file(csv_path+'/movie-actor.csv'))
next(csv_data)
row_count=0
for row in csv_data:
    cur2.execute('INSERT INTO `movie-actor`(movieid, actorid, actor_movie_rank) VALUES(%s, %s, %s)',row)
    db_conn.commit()
    row_count+=1
    if row_count >= 1000:
        db_conn.commit()
        row_count=0

db_conn.commit()
print "movie-actor done"

db_conn.close()
print "All data has been Imported"

