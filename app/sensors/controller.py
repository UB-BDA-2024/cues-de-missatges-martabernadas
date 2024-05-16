from fastapi import APIRouter, Depends, HTTPException,Request
from sqlalchemy.orm import Session
from shared.publisher import Publisher
from shared.database import SessionLocal
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient 
from shared.timescale import Timescale
from shared.cassandra_client import CassandraClient
from shared.sensors import models, schemas, repository
import json
# Dependency to get db session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_timescale():
    ts = Timescale()
    try:
        yield ts
    finally:
        ts.close()

# Dependency to get redis client
def get_redis_client():
    redis = RedisClient(host="redis")
    try:
        yield redis
    finally:
        redis.close()

# Dependency to get mongodb client
def get_mongodb_client():
    mongodb = MongoDBClient(host="mongodb")
    try:
        yield mongodb
    finally:
        mongodb.close()

# Dependency to get elastic_search client
def get_elastic_search():
    es = ElasticsearchClient(host="elasticsearch")
    try:
        yield es
    finally:
        es.close()

# Dependency to get cassandra client
def get_cassandra_client():
    cassandra = CassandraClient(hosts=["cassandra"])
    try:
        yield cassandra
    finally:
        cassandra.close()

router = APIRouter(
    prefix="/sensors",
    responses={404: {"description": "Not found"}},
    tags=["sensors"],
)


# 🙋🏽‍♀️ Add here the route to get a list of sensors near to a given location
@router.get("/near")
def get_sensors_near(latitude: float, longitude: float, radius: float, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client), redis_client: RedisClient = Depends(get_redis_client)):
    return repository.get_sensors_near(mongodb=mongodb_client, latitude=latitude, longitude=longitude, radius=radius,db=db,redis=redis_client)


# 🙋🏽‍♀️ Add here the route to search sensors by query to Elasticsearch
# Parameters:
# - query: string to search
# - size (optional): number of results to return
# - search_type (optional): type of search to perform
# - db: database session
# - mongodb_client: mongodb client
@router.get("/search")
def search_sensors(query: str, size: int = 10, search_type: str = "match", db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client), es: ElasticsearchClient = Depends(get_elastic_search)):
    return repository.search_sensors(db=db,mongodb=mongodb_client, query=query, size=size, search_type=search_type,elastic=es)

# 🙋🏽‍♀️ Add here the route to get the temperature values of a sensor

@router.get("/temperature/values")
def get_temperature_values(mongoDB: MongoDBClient = Depends(get_mongodb_client), cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_temperature_values(mongodb=mongoDB, cassandra=cassandra_client)

@router.get("/quantity_by_type")
def get_sensors_quantity(db: Session = Depends(get_db), cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_sensors_quantity(db=db, cassandra=cassandra_client)

@router.get("/low_battery")
def get_low_battery_sensors(mongoDB: MongoDBClient = Depends(get_mongodb_client), cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_low_battery_sensors(mongodb=mongoDB, cassandra=cassandra_client)

# 🙋🏽‍♀️ Add here the route to get all sensors
@router.get("")
def get_sensors(db: Session = Depends(get_db)):
    return repository.get_sensors(db)


# 🙋🏽‍♀️ Add here the route to create a sensor
@router.post("")
def create_sensor(sensor: schemas.SensorCreate, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client),elastic:ElasticsearchClient=Depends(get_elastic_search),cassandra_client: CassandraClient =Depends(get_cassandra_client)):
    db_sensor = repository.get_sensor_by_name(db, sensor.name)
    if db_sensor:
        raise HTTPException(status_code=400, detail="Sensor with same name already registered")
    return repository.create_sensor(db=db, sensor=sensor,mongoDB=mongodb_client,elastic=elastic,cassandra=cassandra_client)

# 🙋🏽‍♀️ Add here the route to get a sensor by id
@router.get("/{sensor_id}")
def get_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor_mongoDB(mongodb_client, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return db_sensor

# 🙋🏽‍♀️ Add here the route to delete a sensor
@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client),redis:RedisClient=Depends(get_redis_client),elastic:ElasticsearchClient=Depends(get_elastic_search),timescale:Timescale=Depends(get_timescale)):
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.delete_sensor(db=db, sensor_id=sensor_id,mongoDB=mongodb_client,redis=redis,elastic=elastic,timescale=timescale)
    

# 🙋🏽‍♀️ Add here the route to update a sensor
@router.post("/{sensor_id}/data")
def record_data(sensor_id: int, data: schemas.SensorData,db: Session = Depends(get_db) ,redis_client: RedisClient = Depends(get_redis_client),timescale:Timescale=Depends(get_timescale),cassandra_client:CassandraClient=Depends(get_cassandra_client)):
    #Obté el sensor de la base de dades
    db_sensor= repository.get_sensor(db,sensor_id) 
    #Si no el troba llança una excepció
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")    
    #Enregistra les dades del sensor a Redis
    return repository.record_data(redis=redis_client, sensor_id=sensor_id, data=data,timescale=timescale,cassandra=cassandra_client)

# 🙋🏽‍♀️ Add here the route to get data from a sensor
@router.get("/{sensor_id}/data")
def get_data(sensor_id: int,request: Request,db: Session = Depends(get_db) ,redis_client: RedisClient = Depends(get_redis_client),timescale:Timescale=Depends(get_timescale)):    
    #Obté el sensor de la base de dades
    db_sensor= repository.get_sensor(db,sensor_id)
    #Si no el troba llança una excepció
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    # Obtenim els paràmetres from,to i bucket de la petició 
    from_date = request.query_params.get('from', None)
    to_date = request.query_params.get('to', None)
    bucket = request.query_params.get('bucket', None)
    # Actualitza les dades del sensor amb la informació obtinguda de Redis
    return repository.get_data(timescale=timescale,redis=redis_client, sensor_id=sensor_id,sensor_name=db_sensor.name,from_date=from_date,to_date=to_date,bucket=bucket)

publisher = Publisher()

class ExamplePayload():
    def __init__(self, example):
        self.example = example

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
@router.post("/exemple/queue")
def exemple_queue():
    # Publish here the data to the queue
    publisher.publish(ExamplePayload("holaaaaa"))
    return {"message": "Data published to the queue"}