class User:
    user_id = 0
    age = 0
    occupation = 0

    def __init__(cls, user_id, gender, age, occupation, zip_code):
        cls.user_id = int(user_id)
        cls.gender = gender
        cls.age = int(age)
        cls.occupation = int(occupation)
        cls.zip_code = zip_code

class Movie:
    movie_id = 0

    def __init__(self, movie_id, title, genres):
        self.movie_id = int(movie_id)
        self.title = title
        self.genres = genres