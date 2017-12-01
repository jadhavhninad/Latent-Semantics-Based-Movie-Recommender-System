from mysqlConn import DbConnect
import argparse
from numpy import *
import operator
import pandas as pd
from get_relevance import movie_rel_ratio_DF



#DB connector and curosor
db = DbConnect()
db_conn = db.get_connection()
cur2 = db_conn.cursor();

#Argument parser
parser = argparse.ArgumentParser()
parser.add_argument("USER")
args = parser.parse_args()


#=====================================================================
#Task:7 - Generate a rating of all movies for a user and sort them
#=====================================================================

#After reconstruction, we loose the column and row header names. so we need to do
#some mapping.

with open("R_final_svd_70k.csv") as f:
    ncols = len(f.readline().split('\t'))

R_final = pd.DataFrame(loadtxt('R_final_svd_70k.csv',delimiter='\t', skiprows=1, usecols=range(1,ncols)))


#Get user_ids and movie_ids

with open("user_ids_svd_70k.csv") as f:
    ncols_u = len(f.readline().split('\t'))

with open("movie_ids_svd_70k.csv") as f:
    ncols_m = len(f.readline().split('\t'))

user_list = list(loadtxt('user_ids_svd_70k.csv',delimiter='\t', skiprows=1, usecols=range(1,ncols_u)))
movie_list = list(loadtxt('movie_ids_svd_70k.csv',delimiter='\t', skiprows=1, usecols=range(1,ncols_m)))

#print user_list
#print movie_list
user_id = args.USER
user_index = user_list.index(float(user_id))

#print R_final.columns[user_index]

movie_itr = 0
movie_recommendations={}

for index, row in R_final.iterrows():
    #print row[user_index]
    #Map the generated rating values for a user to a given movie. Get a ceiling rating.
    #print movie_list[movie_itr]
    movie_recommendations[movie_list[movie_itr]] = abs(ceil(row[user_index]))
    if movie_recommendations[movie_list[movie_itr]] <= 0:
        movie_recommendations[movie_list[movie_itr]] = 0.5
        # Such movies will be recommended below the 1 rating movie

    #print movie_recommendations[movie_list[movie_itr]]
    movie_itr+=1

#print "--------Generated movie recommendations---------"
#print movie_recommendations

#============================================================
#Get a list of user watched movies
#============================================================

userWatchedMovies = []
cur2.execute("SELECT movieid_id FROM `phase3_mlratings` where userid_id = %s",[args.USER])
result0 = cur2.fetchall()
for data in result0:
    #print type(data[0])
    userWatchedMovies.append(data[0])

cur2.execute("SELECT movieid_id FROM `phase3_mltags` where userid_id = %s",[args.USER])
result0 = cur2.fetchall()
for data in result0:
    userWatchedMovies.append(data[0])

print "-----Watched movies-------"
for watched_ids in userWatchedMovies:
    cur2.execute("SELECT moviename,genres FROM `phase3_mlmovies` where movieid = %s", {watched_ids, })
    print cur2.fetchone()

#print "----Watched movies-------"
#print userWatchedMovies

    #=======================================================
    #Filter out ratings of watched movies
    #=======================================================
    userNotWatched={}

    for mv_id in movie_recommendations:
        mid = str(int(mv_id))
        #print type(mid)
        if mid in userWatchedMovies:
            #print "watched found"
            continue
        else:
            userNotWatched[mid] = movie_recommendations[mv_id]

    #print "---- movies not watched---"
    #print userNotWatched


while (True):
    #print "----Sorted movie recommendations-----"
    movie_recommendations_sorted = sorted(userNotWatched.items(), key=operator.itemgetter(1), reverse=True)
    #print movie_recommendations_sorted

    #Return top 5 unwatched movies in the generated recommendations
    user_feedback = {}
    print "-------Top 5 Recommended movies------"
    for i in range(0,5,1):
        print movie_recommendations_sorted[i]
        cur2.execute("SELECT moviename,genres FROM `phase3_mlmovies` where movieid like %s", {movie_recommendations_sorted[i][0], })
        print cur2.fetchone()
        user_feedback[movie_recommendations_sorted[i][0]] = 0;
    
    
    print "--------------ALL MOVIE WEIGHTS ---------------------"
    for key in movie_recommendations_sorted:
        print key

    print "-------Submit your feedback (relevant :'1', irrelevant :'0')-------- : "
    #Get user FeedBack and store corresponding ids in DICT
    k=1
    for key in user_feedback:
        print "Movie", k
        user_feedback[key] = raw_input("Feedback : ")
        k+=1

    #Get the probabilistic relevance feedback values for all movies (prf)
    prf_movie = movie_rel_ratio_DF(user_feedback)

    #Update the Rating vals:
    for key in userNotWatched:
        if key in prf_movie:
            userNotWatched[key] *= prf_movie[key]
