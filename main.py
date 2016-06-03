from pyspark import sql
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
from pyspark import SparkContext, SparkConf
from load import load_movies, load_users, load_ratings
from parsers import parse_rating
from models import *

import os

TRAINING_SHARE = 0.8
TEST_SHARE = 0.2

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

    #splitted_dataset = rating_rdd.randomSplit((TRAINING_SHARE, TEST_SHARE),0)
    #training_ratings_rdd = splitted_dataset[0].cache()
    #print "Training set:", training_ratings_rdd.count()

    #test_ratings_rdd = splitted_dataset[1].cache()
    #print "Test set:", test_ratings_rdd.count()

    training_rdd, test_rdd = split_dataset(rating_rdd)
    model = model_training(training_rdd)
    test = model_testing(model, test_rdd, training_rdd)
    print test

    print recommend_movies(model, 4169, 5)

    #model = ALS.train(training_ratings_rdd, rank=20, iterations=10)

    #top_5_recs = model.recommendProducts(4169,num=5)
    #print top_5_recs

    #test_user_product = test_ratings_rdd.map(lambda rating: (rating[0], rating[1]))
    #predictions = model.predictAll(test_user_product).map(lambda rating: (rating[0], rating[1], rating[2]))
    #rates_and_predictions = training_ratings_rdd.map(lambda rating: (rating[0], rating[1], rating[2])).join(predictions)
    #rms_error = rates_and_predictions.map(lambda rating: (rating[1][0]-rating[1][1])**2).mean()
    #print rms_error

    print 'Done'