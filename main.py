from pyspark import sql
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
from pyspark import SparkContext, SparkConf
from load import load_movies, load_users, load_ratings

from parsers import parse_rating

import os

os.environ['SPARK_HOME'] = "/opt/spark-1.6.0-bin-hadoop2.4"

def counting_the_rdd(rdd):
    num_ratings = rdd.count()
    print "# ratings:", int(num_ratings)
    num_movies = rdd.map(lambda Rating: Rating.product).distinct().count()
    print "# movies:", int(num_movies)
    num_users = rdd.map(lambda Rating: Rating.user).distinct().count()
    print "# users:", int(num_users)

def querying_the_dataframe(sql_context):
    top_rated = """
        SELECT
            movies.title, movierates.maxr, movierates.minr, movierates.cntu
        FROM
            (
                SELECT
                    product,
                    MAX(rating) AS maxr,
                    MIN(rating) AS minr,
                    COUNT(DISTINCT user) AS cntu
                FROM
                    ratings
                GROUP BY
                    product
            )
            movierates JOIN movies ON movierates.product = movies.movie_id
        ORDER BY movierates.cntu DESC
    """
    sql_context.sql(top_rated).show()

    most_active_users_query = """
        SELECT
            ratings.user,
            COUNT(*) AS ct
        FROM
            ratings
        GROUP BY
            ratings.user
        ORDER BY
            ct DESC
        LIMIT 10
    """
    sql_context.sql(most_active_users_query)

    higher_than_four_query = """
        SELECT
            ratings.user,
            ratings.product,
            ratings.rating,
            movies.title
        FROM
            ratings JOIN movies ON movies.movie_id = ratings.product
        WHERE
            ratings.user = 4169
            AND ratings.rating > 4
    """
    sql_context.sql(higher_than_four_query).show()

if __name__ == "__main__":
    conf = SparkConf().setAppName('RecommenderWithSpark')
    sc = SparkContext(conf=conf)

    #counting_the_rdd(rating_rdd)

    sql_context = sql.SQLContext(sc)

    #users_df, users_rdd = load_users(sc, sql_context)
    #movies_df, movies_rdd = load_movies(sc, sql_context)
    #ratings_df, ratings_rdd = load_ratings(sc, sql_context)

    #querying_the_dataframe(sql_context)
    rating_text = sc.textFile("datasets/ratings.dat")
    rating_rdd = rating_text.map(lambda line: parse_rating(line)).cache()

    splitted_dataset = rating_rdd.randomSplit((0.8, 0.2),0)



    print 'Done'