from pymongo import MongoClient

class MakeConnection:

    def __init__(self):
        self.client = MongoClient("CREDS HERE")
        self.db = self.client["Web_Scraping"]
        self.collection = self.db["movie_user_rating_dating"]
        print('connection_made')
#        collection.insert_one({"test": "working"})
