from pymongo import MongoClient


class DB:
    def __init__(self, db_name, collection_name):
        self.client = MongoClient()
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def put_many(self, documents):
        return self.collection.insert_many(documents).inserted_ids

    def put(self, document):
        return self.collection.insert_one(document).inserted_id

    def get_ids(self):
        return self.collection.find({}, {"account_id": 1, "_id": 0})

