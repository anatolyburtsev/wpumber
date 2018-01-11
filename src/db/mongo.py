from pymongo import MongoClient


class DB:
    def __init__(self, db_name, collection_name):
        self.client = MongoClient()
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def put_many(self, documents):
        return self.collection.insert_many(documents).inserted_ids
