import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import redis

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")

client = AsyncIOMotorClient(MONGO_URI)
db = client[DATABASE_NAME]

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
    print("Conectado ao Redis com sucesso!")
except redis.exceptions.ConnectionError as e:
    print(f"ERRO: Não foi possível conectar ao Redis. Verifique se o servidor está rodando. Erro: {e}")
    redis_client = None