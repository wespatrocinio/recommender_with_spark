from models import Movie, User
from pyspark.mllib.recommendation import Rating

def parse_movie(line):
    fields = line.split("::")
    return Movie(*fields)

def parse_user(line):
    fields = line.split("::")
    return User(*fields)

def parse_rating(line):
    fields = line.split("::")
    return Rating(*fields[:-1])