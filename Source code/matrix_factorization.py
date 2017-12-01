from mysqlConn import DbConnect
import pandas as pd

#DB connector and curosor
db = DbConnect()
db_conn = db.get_connection()
cur2 = db_conn.cursor();

def get_user_mvrating_DF():
    #===========================================================================
    #Generate user - movie_rating matrix.
    #For each movie, get its rating given by a user. If no rating then give zero
    #===========================================================================

    dd_users_mvrating = {}
    dd_av_rating_for_genre = {}
    dd_total_movie_for_genre = {}

    #Limit is for checking that algorithm works.
    cur2.execute("SELECT userid FROM `phase3_mlusers` where userid>70000")
    result0 = cur2.fetchall();
    for usr in result0:
        #print "for user" , usr[0]
        dd_users_mvrating[usr[0]] = {}
        dd_av_rating_for_genre[usr[0]] = {}
        dd_total_movie_for_genre[usr[0]] = {}

        #Get all movies watched(and hence rated) by each user.
        cur2.execute("SELECT movieid_id, rating FROM `phase3_mlratings` where userid_id = %s",usr)
        result1 = cur2.fetchall()
        for data1 in result1:

            user_movie_id = data1[0]
            user_movie_rating = data1[1]

            if user_movie_id in dd_users_mvrating[usr[0]]:
                continue
            else:
                #print user_movie_id,user_movie_rating
                dd_users_mvrating[usr[0]][user_movie_id] = user_movie_rating

            #mlmovies_clean maps one movie to a single genre.
            #Get the genre of the movie and add the movie rating to the genre.
            cur2.execute("SELECT genres FROM `phase3_mlmovies_clean` where movieid = %s", {user_movie_id,})
            result_gen = cur2.fetchall()
            for data in result_gen:
                if data[0] in dd_av_rating_for_genre[usr[0]]:
                    dd_av_rating_for_genre[usr[0]][data[0]] += user_movie_rating
                    dd_total_movie_for_genre[usr[0]][data[0]] += 1
                else:
                    dd_av_rating_for_genre[usr[0]][data[0]] = user_movie_rating;
                    dd_total_movie_for_genre[usr[0]][data[0]] = 1


        #Now for every genre of which the user has seen a movie we have the total rating of that genre
        #We can get the average rating given to a particular genre by a user.

        #WE need give rating to movies of mltags because it does not have a rating,

        # give rating = avg rating give to a particular genre to by a user.

        cur2.execute("SELECT movieid_id FROM `phase3_mltags` where userid_id = %s", usr)
        result2 = cur2.fetchall()
        for data in result2:
            #print data1
            user_movie_id = data[0]
            cur2.execute("SELECT genres FROM `phase3_mlmovies_clean` where movieid = %s", {user_movie_id, })
            mv_genre = cur2.fetchall()

            if user_movie_id in dd_users_mvrating[usr[0]]:
                continue
            else:
                #print user_movie_id
                val = 0.0
                for gen in mv_genre:
                    if gen in dd_av_rating_for_genre[usr[0]]:
                        val += float(dd_av_rating_for_genre[usr[0]][gen])/float(dd_total_movie_for_genre[usr[0]][gen])
                    else:
                        val = 1.0

                dd_users_mvrating[usr[0]][user_movie_id] = val/float(len(mv_genre))


        #Make rating of other movies to zero.
        cur2.execute("SELECT DISTINCT movieid FROM `phase3_mlmovies`")
        genreNames = cur2.fetchall()

        for keyval in genreNames:
            key = keyval[0]
            #print key
            if key in dd_users_mvrating[usr[0]]:
                continue
            else:
                dd_users_mvrating[usr[0]][key] = 0



    #pprint.pprint(dd_users_genre)
    usr_mvrating_matrix = pd.DataFrame(dd_users_mvrating)

    #print list(usr_mvrating_matrix.columns.values)
    #print list(usr_mvrating_matrix.index)

    user_ids_df = pd.DataFrame(usr_mvrating_matrix.columns.values, columns=["user_ids"] )
    movie_ids_df = pd.DataFrame(usr_mvrating_matrix.index, columns=["movie_ids"] )

    user_ids_df.to_csv("user_ids_svd.csv",sep="\t")
    movie_ids_df.to_csv("movie_ids_svd.csv", sep="\t")

    return usr_mvrating_matrix

if __name__ == "__main__":
    usr_mvrating_matrix = get_user_mvrating_DF()
    # usr_genre_matrix = usr_genre_matrix.T
    # pprint.pprint(usr_genre_matrix)
    usr_mvrating_matrix.to_csv("factorization_1_user_mvrating_svd_usr_grt_70k.csv", sep='\t')












