from pymongo import MongoClient

class MongoDBClient:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = MongoClient(host, port)
        self.database = None
        self.collection = None

    def close(self):
        self.client.close()
    
    def ping(self):
        return self.client.db_name.command('ping')
    
    def getDatabase(self, database):
        self.database = self.client[database]
        return self.database

    def getCollection(self, collection):
        self.collection = self.database[collection]
        return self.collection
    
    def clearDb(self,database):
        self.client.drop_database(database)
        
     # Funció per inserir un document a la col·lecció 
    def insertDocument(self,document):
        return self.collection.insert_one(document)
    
    # Funció per esborrar un document de la col·lecció
    def deleteDocument(self,query):
        self.collection.delete_one(query)

    # Funció per obtenir els documents de la col·lecció 
    def getDocuments(self,query):
        return self.collection.find(query, {'_id': 0})
    
    # Funció per obtenir un document de la col·lecció 
    def getDocument(self,query):
        return self.collection.find_one(query, {'_id': 0})

