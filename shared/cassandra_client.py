from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()
        # Crea el keyspace "sensor" si ºno existeix
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS sensor WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1};")
        # Crea les taules per a les dades de temperatura, quantitat de sensors de cada tipus i bateria si no existeixen
        self.session.execute("CREATE TABLE IF NOT EXISTS sensor.temperature(id INT, temperature FLOAT, PRIMARY KEY(id, temperature));")
        self.session.execute("CREATE TABLE IF NOT EXISTS sensor.quantity(id INT, type text, PRIMARY KEY(type, id));")
        self.session.execute("CREATE TABLE IF NOT EXISTS sensor.battery(id INT, battery_level FLOAT, PRIMARY KEY(battery_level, id));")
    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)