from parsers import parse_movie, parse_user, parse_rating

def load_users(sc, sql_context):
    users_text = sc.textFile("datasets/users.dat")
    users_rdd = users_text.map(lambda line: parse_user(line))
    users_df = sql_context.createDataFrame(users_rdd)
    users_df.printSchema()
    users_df.registerTempTable("users")
    return users_df, users_rdd

def load_movies(sc, sql_context):
    movies_text = sc.textFile("datasets/movies.dat")
    movies_rdd = movies_text.map(lambda line: parse_movie(line))
    movies_df = sql_context.createDataFrame(movies_rdd)
    movies_df.printSchema()
    movies_df.registerTempTable("movies")
    return movies_df, movies_rdd

def load_ratings(sc, sql_context):
    rating_text = sc.textFile("datasets/ratings.dat")
    rating_rdd = rating_text.map(lambda line: parse_rating(line)).cache()
    ratings_df = sql_context.createDataFrame(rating_rdd)
    ratings_df.printSchema()
    ratings_df.registerTempTable("ratings")
    return ratings_df, rating_rdd