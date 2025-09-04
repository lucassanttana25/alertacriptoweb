import pymongo
from faker import Faker
import random
from dotenv import load_dotenv
import os
from datetime import datetime
import os

from neo4j import GraphDatabase

load_dotenv()

# --- Conexão com o MongoDB Atlas ---
load_dotenv()

# --- 1. Conexão com os Bancos de Dados ---

# MongoDB Atlas
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "alertacripto_db"
COLLECTION_NAME = "alerts"
mongo_client = pymongo.MongoClient(MONGO_URI)

# Neo4j AuraDB
NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USR")
NEO4J_PASSWORD = os.environ.get("NEO4J_Psw")
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# --- 2. Extração, Transformação e Carga (ETL) ---

try:
    mongo_client = pymongo.MongoClient(MONGO_URI)
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    # Verifica as conexões
    mongo_client.server_info()
    neo4j_driver.verify_connectivity()
    print("Conexões com MongoDB e Neo4j estabelecidas com sucesso.")

    # --- 2. Extração, Transformação e Carga (ETL) ---

    db = mongo_client[DB_NAME]
    alerts_collection = db[COLLECTION_NAME]

    alerts_documents = alerts_collection.find({})

    with neo4j_driver.session() as session:
        print("\nIniciando a migração dos alertas...")

        for doc in alerts_documents:
            # Transforma o documento do MongoDB em um nó de grafo
            session.run(
                "MERGE (a:Alert {mongoId: $id}) "
                "SET a.assetTicker = $assetTicker, a.condition = $condition, a.targetPrice = $targetPrice, a.status = $status",
                id=str(doc['_id']),
                assetTicker=doc.get('assetTicker'),
                condition=doc.get('condition'),
                targetPrice=doc.get('targetPrice'),
                status=doc.get('status')
            )

            # Encontra o usuário correspondente e cria o relacionamento
            session.run("""
                MATCH (u:User {mongo_id: $user_id})
                MATCH (a:Alert {mongoId: $alert_id})
                MERGE (u)-[:SET_ALERT]->(a)
            """, user_id=str(doc.get('userId')),
                        alert_id=str(doc['_id'])
            )


    print("\nTodos os alertas e seus relacionamentos foram migrados para o Neo4j com sucesso.")

except Exception as e:
    print(f"Ocorreu um erro durante a migração: {e}")

finally:
    if 'mongo_client' in locals():
        mongo_client.close()
    if 'neo4j_driver' in locals():
        neo4j_driver.close()
    print("Conexões com os bancos de dados fechadas.")
