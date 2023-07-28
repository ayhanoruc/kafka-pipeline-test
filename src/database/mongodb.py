import pymongo
import os 
import certifi

ca= certifi.where()

url= os.getenv('MONGO_DB_URL')
class MongodbOperation:

    def __init__(self) -> None:
        
        self.client= pymongo.MongoClient(url,tlsCAFile=ca)
        self.db_name = "ayhanoruc"

    
    def insert_many(self,collection_name,records:list):
        self.client[self.db_name][collection_name].insert_many(records)


    def insert(self,collection_name,record):
        self.client[self.db_name][collection_name].insert_one(record)

    