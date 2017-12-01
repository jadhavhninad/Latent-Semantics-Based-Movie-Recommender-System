from mysqlConn import DbConnect
import pandas as pd
import math

#DB connector and curosor
db = DbConnect()
db_conn = db.get_connection()
cur2 = db_conn.cursor();

def movie_rel_ratio_DF(mv_relevance):
    rel_mv = []
    irr_mv = []
    genre_rel_count={}
    genre_total_count={}
    p={}
    u={}
    mv_rel_sum = {}

    #Separate the relevant and irrelevant movies
    for mv in mv_relevance:
        if mv_relevance[mv] == 0:
            irr_mv.append(mv)
        else:
            rel_mv.append(mv)

    #Get count r for a genre i.e number of relevant movies that have a genre
    for mv in rel_mv:
        cur2.execute("SELECT genres FROM `phase3_mlmovies_clean` where movieid = %s", {mv})
        result_gen = cur2.fetchall()

        for val in result_gen:
            if val[0] in genre_rel_count:
                genre_rel_count[val[0]]+=1
            else:
                genre_rel_count[val[0]] = 1

    #get count of all movies that have a genre
    for mv in mv_relevance:
        cur2.execute("SELECT genres FROM `phase3_mlmovies_clean` where movieid = %s", {mv})
        result_gen = cur2.fetchall()

        for val in result_gen:
            if val[0] in genre_total_count:
                genre_total_count[val[0]]+=1
            else:
                genre_total_count[val[0]] = 1



    #R = total number of relevant retrieved items
    R = float(len(rel_mv))

    #N = total number of items
    N = 5

    #Get relevant and irrevelant probabilities
    for gen in genre_rel_count:
        p[gen] = float((genre_rel_count[gen] + 0.5 ) / (R + 1))
        u[gen] = float((genre_total_count[gen] - genre_rel_count[gen] + 0.5) / (N - R + 1))
        #print "p = ",p[gen]," u = ",u[gen]



    cur2.execute("SELECT Distinct(movieid) FROM `phase3_mlmovies_clean`")
    result_mv = cur2.fetchall()
    for mv_id in result_mv:
        cur2.execute("SELECT genres FROM `phase3_mlmovies_clean` where movieid = %s", {mv_id})
        result_gen = cur2.fetchall()

        mv_rel_sum[mv_id[0]] = 0;

        for genKey in result_gen:
            gen = genKey[0]
            if gen in genre_rel_count:
                mv_rel_sum[mv_id[0]] += math.log(float(p[gen]*(1-u[gen])) / (u[gen]*(1-p[gen])))
            else:
                continue

    #change this
    return mv_rel_sum

if __name__ == "__main__":
    my_mvs = {}
    my_mvs['9739'] = 0
    my_mvs['6097'] = 1
    my_mvs['3189'] = 1
    my_mvs['8901'] = 0
    my_mvs['5000'] = 1
    retVal = movie_rel_ratio_DF(my_mvs)
    for key in retVal:
        print key, "=", retVal[key]

