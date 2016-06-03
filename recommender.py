from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel
from pyspark import SparkContext, SparkConf
from parsers import parse_rating

TRAINING_SHARE = 0.8
TEST_SHARE = 0.2

ALS_RANK = 20
ALS_NUM_ITERATIONS = 10

MODEL_PATH = "target/collaborative_filter"
DATASET_PATH = "datasets/ratings.dat"

def load_dataset(sc):
    rating_text = sc.textFile(DATASET_PATH)
    rating_rdd = rating_text.map(lambda line: parse_rating(line)).cache()

def split_dataset(dataset):
    splitted_dataset = dataset.randomSplit((TRAINING_SHARE, TEST_SHARE),0)
    training_dataset = splitted_dataset[0].cache()
    print "Training set size:", training_dataset.count()
    test_dataset = splitted_dataset[1].cache()
    print "Test set size:", test_dataset.count()
    return training_dataset, test_dataset

def model_training(training_set):
    return ALS.train(training_set, rank=ALS_RANK, iterations=ALS_NUM_ITERATIONS)

def model_testing(model, test_rdd, training_rdd):
    test_user_product = test_rdd.map(lambda rating: (rating[0], rating[1]))
    predictions = model.predictAll(test_user_product).map(lambda rating: (rating[0], rating[1], rating[2]))
    rates_and_predictions = training_rdd.map(lambda rating: (rating[0], rating[1], rating[2])).join(predictions)
    return rates_and_predictions.map(lambda rating: (rating[1][0] - rating[1][1]) ** 2).mean()

def recommend_movies(model, user, num_recs):
    return model.recommendProducts(user,num=num_recs)

def save_model(model, sc):
    return model.save(sc, MODEL_PATH)

def load_model(sc):
    return MatrixFactorizationModel.load(sc, MODEL_PATH)