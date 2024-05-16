from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.cassandra_client import CassandraClient
from shared.timescale import Timescale
import json

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongoDB: MongoDBClient,elastic:ElasticsearchClient,cassandra:CassandraClient) -> models.Sensor:
    #Crea el sensor i l'emmagatzema a PostgreSQL
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    #Accedeix a la base de dades DB i a la col·lecció Sensors
    mongoDB.getDatabase('DB')
    collection=mongoDB.getCollection('sensors')

    #Crea el document amb la informació del sensor
    document = {
        "id": db_sensor.id,
        "name": sensor.name,
        "type": sensor.type,
        "longitude":sensor.longitude,
        "latitude":sensor.latitude,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        },
        "description": sensor.description
    }

    # Crea un índex per la ubicació
    collection.create_index([("location","2dsphere")])

    #Afegeix el document a mongoDB
    mongoDB.insertDocument(document)
    #Indexem els camps nom, descripció i tipus de sensor a l'índex extern
    data = {
        'name': sensor.name,
        'type': sensor.type,
        'description': sensor.description
    }
    elastic.index_document('sensors', data)
    #Guardem el id i el tipus del sensor a la taula quantity de cassandra
    query = f"INSERT INTO sensor.quantity(id, type) VALUES ({db_sensor.id}, '{sensor.type}');"
    cassandra.execute(query)

    #Afegim l'id   
    result=sensor.dict()
    result['id']=db_sensor.id
    return result


def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData,timescale:Timescale,cassandra:CassandraClient) -> schemas.Sensor:
    # Crea un diccionari amb les dades del sensor
    sensor_data={
        "velocity":data.velocity,
        "temperature": data.temperature,
        "humidity":data.humidity,
        "battery_level":data.battery_level,
        "last_seen":data.last_seen
    }
    #Transformem les dades per a que puguin ser inserides a la query de SQL, ja que no accepta None i ho hem de substituir per Null        
    ts_data = {key: value if value is not None else 'NULL' for key, value in sensor_data.items()}
    ts_data['last_seen'] = f"'{ts_data['last_seen']}'"
    
    # Afegeix les dades a TimescaleDB
    timescale_query = f"""
    INSERT INTO sensor_data (id, temperature, humidity, velocity, battery_level, last_seen) 
    VALUES ({sensor_id}, {ts_data['temperature']}, {ts_data['humidity']}, {ts_data['velocity']}, {ts_data['battery_level']}, {ts_data['last_seen']})
    ON CONFLICT (id, last_seen) DO UPDATE 
    SET temperature = EXCLUDED.temperature,
        humidity = EXCLUDED.humidity,
        velocity = EXCLUDED.velocity,
        battery_level = EXCLUDED.battery_level;
    """
    timescale.execute(timescale_query)
    timescale.execute("commit")

    #Si el sensor té dades de temperatura les guardem a la taula de temperatura de cassandra
    if data.temperature is not None:
        query=f"""
            INSERT INTO sensor.temperature
            (id, temperature)
            VALUES ({sensor_id}, {data.temperature});
            """
        cassandra.execute(query)
    #Guardem el nivell de bateria a la taula bateria de cassandra
    query=f"""
        INSERT INTO sensor.battery
        (id, battery_level)
        VALUES ({sensor_id}, {data.battery_level});
        """
    cassandra.execute(query)
    # Passa les dades a JSON i les emmagatzema a Redis 
    redis.set(sensor_id, json.dumps(sensor_data))
    return data

def get_data(redis: RedisClient, sensor_id: int,sensor_name:str,timescale:Timescale,from_date:str,to_date:str,bucket:str) -> schemas.Sensor:
    if from_date is None and to_date is None and bucket is None:
        #Obté les dades del sensor de Redis
        db_sensordata = json.loads(redis.get(sensor_id))

        #Si no les troba llança una excepció
        if db_sensordata is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
            
        # Afegeix l'identificador i el nom del sensor a les dades obtingudes de Redis
        db_sensordata['id']=sensor_id
        db_sensordata['name']=sensor_name

        return db_sensordata
    else:
        # Creem la query per obtenir dades del sensor agrupades per intervals de temps
        query = f"""
            SELECT 
                id,
                time_bucket('1 {bucket}', last_seen) AS {bucket},
                AVG(velocity) AS velocity,
                AVG(temperature) AS temperature,
                AVG(humidity) AS humidity
            FROM sensor_data
            WHERE id = {sensor_id} AND last_seen >= '{from_date}' AND last_seen <= '{to_date}'
            GROUP BY id, {bucket};
        """
        #Executem la query i retornem els resultats obtinguts
        timescale.execute(query)
        result = timescale.getCursor().fetchall()
        return result

def delete_sensor(db: Session, sensor_id: int,mongoDB:MongoDBClient,redis:RedisClient,elastic:ElasticsearchClient,timescale:Timescale):
    #Obté el sensor de postgreSQL
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

    #Si no existeix llança una excepció
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    #Elimina el sensor de postgreSQL
    db.delete(db_sensor)
    db.commit()

    #Elimina el document de mongoDB
    mongoDB.getDatabase('DB')
    mongoDB.getCollection('sensors')
    mongoDB.deleteDocument({"id": sensor_id})

    #Elimina la clau de redis
    redis.delete(sensor_id)
    return db_sensor
def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float,radius:float,redis:RedisClient,db:Session) -> List:
    #Accedeix a la base de dades i la col·lecció de mongoDB
    mongodb.getDatabase('DB')
    mongodb.getCollection('sensors')

    # Crea una query per obtenir els sensors que tinguin els valors de longitud i latitud dins del radi establert
    query = {"latitude": {"$gte": latitude - radius, "$lte": latitude + radius},"longitude": {"$gte": longitude - radius, "$lte": longitude + radius}}
    
    #Recuperem els documents que compleixin la condició 
    sensors_near = list(mongodb.getDocuments(query))
    print(sensors_near)
    #Per cada document obtigut actualitzem les seves dades
    for sensor in sensors_near:
        #Obtenim les dades de postgreSQL
        db_sensor=get_sensor(db=db,sensor_id=sensor['id'])
        #Obtenim les dades de redis
        db_data=get_data(redis=redis,sensor_id=db_sensor.id,sensor_name=db_sensor.name)
        #Les afegim al document
        sensor['velocity']=db_data['velocity']
        sensor['temperature']=db_data['temperature']
        sensor['humidity']=db_data['humidity']
        sensor['battery_level']=db_data['battery_level']
        sensor['last_seen']=db_data['last_seen']
    
    return sensors_near
def get_sensor_mongoDB(mongoDB:MongoDBClient,sensor_id:int)->schemas.Sensor:
    #Accedeix a la base de dades i la col·lecció de mongoDB
    mongoDB.getDatabase('DB')
    mongoDB.getCollection('sensors')
    #Retorna el document amb els camps longitud i latitud
    document=mongoDB.getDocument({'id': sensor_id})
    document['longitude']=document['location']['coordinates'][0]
    document['latitude']=document['location']['coordinates'][1]
    del document['location']
    return document

def search_sensors(db:Session,mongodb:MongoDBClient, query:str, size:int, search_type:str,elastic:ElasticsearchClient):
    # Si el tipus de cerca és "similar", el convertim a "fuzzy", perquè "similar" no és una consulta vàlida en Elasticsearch.
    if search_type == "similar":
        search_type = "fuzzy"
    #Crea la query de cerca
    query = {
        "query": {
            search_type: json.loads(query)
        }
    }
    #Fa la cerca i guarda els resultats
    results = elastic.search(index_name="sensors", query=query)
    sensors = []
    #Itera pels n primers resultats on n és el size que passem per paràmetre, obté els sensors de la base de dades i els afegeix en una llista
    for hit in results["hits"]["hits"][:size]:
        name_sensor = hit["_source"]["name"]
        sensordb=get_sensor_by_name(db,name_sensor)
        sensor = get_sensor_mongoDB(mongodb,sensordb.id)
        sensors.append(sensor)
    return sensors
def get_temperature_values(mongodb: MongoDBClient, cassandra: CassandraClient):
    # Obtenim el valor màxim de temperatura, el mínim i la mitja de les temperatures de la taula temperature per cada sensor
    query = """
        SELECT id, 
        MAX(temperature) AS max_temperature, 
        MIN(temperature) AS min_temperature, 
        AVG(temperature) AS avg_temperature
        FROM sensor.temperature
        GROUP BY id;
    """
    results = cassandra.execute(query)
    sensors = []
    for row in results:
        #Obtenim totes les dades del sensor 
        sensor=get_sensor_mongoDB(mongodb,row.id)
        #Hi afegim el valor mínim i màxim de temperatura i la mitja
        sensor['values'] = {'max_temperature': row.max_temperature,'min_temperature': row.min_temperature,'average_temperature': row.avg_temperature}
        sensors.append(sensor)
    return {'sensors': sensors}


def get_sensors_quantity(db: Session, cassandra: CassandraClient):
    # Obtenim la quantitat de sensors que hi ha de cada tipus
    query = """
        SELECT type, count(*) AS quantity
        FROM sensor.quantity
        GROUP BY type;
    """
    result = cassandra.execute(query)
    sensors = []
    for row in result:
        #Afegim el tipus de sensor i el nombre de sensors que hi ha d'aquell tipus
        sensors.append({"type": row.type, "quantity": row.quantity})
    return {'sensors': sensors}

def get_low_battery_sensors(mongodb: MongoDBClient, cassandra: CassandraClient):
    # Obtenim els sensors que tenen el nivell de bateria per sota del 20%
    query = """
        SELECT id, battery_level
        FROM sensor.battery
        WHERE battery_level < 0.2
        ALLOW FILTERING;
    """
    results = cassandra.execute(query)
    sensors = []

    for row in results:
        #Obtenim totes les dades del sensor 
        sensor=get_sensor_mongoDB(mongodb,row.id)
        #Hi afegim el nivell de bateria
        sensor.update({"battery_level": round(row.battery_level, 2)})
        sensors.append(sensor)
    print(sensors)
    return {'sensors': sensors}