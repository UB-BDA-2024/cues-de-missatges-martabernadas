import fastapi
from .sensors.controller import router as sensorsRouter
import yoyo

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

app.include_router(sensorsRouter)

#TODO: Apply new TS migrations using Yoyo
#Read docs: https://ollycope.com/software/yoyo/latest/

# Establim la connexió amb la base de dades PostgreSQL
backend = yoyo.get_backend("postgresql://timescale:timescale@timescale:5433/timescale")

# Llegim les migracions des del directori "migrations_ts"
migrations = yoyo.read_migrations("migrations_ts")

# Bloqueig per garantir que les migracions s'apliquin de manera segura
with backend.lock():
    # Apliquem les migracions pendents a la base de dades
    backend.apply_migrations(backend.to_apply(migrations))
@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
