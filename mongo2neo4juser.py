import pymongo
from faker import Faker
import random
from dotenv import load_dotenv
import os
from datetime import datetime
import os

from neo4j import GraphDatabase

load_dotenv()

load_dotenv()


# MongoDB Atlas
MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "alertacripto_db"
COLLECTION_NAME = "users"
mongo_client = pymongo.MongoClient(MONGO_URI)

# Neo4j AuraDB
NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USER = os.environ.get("NEO4J_USR")
NEO4J_PASSWORD = os.environ.get("NEO4J_Psw")
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def migrate_users_to_neo4j():
    try:
        # Verifica a conectividade antes de iniciar
        mongo_client.server_info()
        neo4j_driver.verify_connectivity()
        print("Conexões com MongoDB e Neo4j estabelecidas com sucesso.")

        db = mongo_client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Lê todos os documentos da coleção de usuários
        users_documents = collection.find({})

        with neo4j_driver.session() as session:
            for doc in users_documents:
                print(doc)
                session.run(
                    "MERGE (u:User {email: $email}) "
                    "SET u.name = $name, u.risk_profile = $risk_profile, u.mongo_id = $mongo_id",
                    email=doc.get('email'),
                    name=doc.get('name'),
                    risk_profile=doc.get('risk_profile'),
                    mongo_id=str(doc['_id'])
                )

        print("\nTodos os usuários foram migrados para o Neo4j com sucesso.")

    except Exception as e:
        print(f"Ocorreu um erro durante a migração: {e}")

    finally:
        mongo_client.close()
        neo4j_driver.close()
        print("Conexões com os bancos de dados fechadas.")

# --- Execução do Script ---
if __name__ == "__main__":
    migrate_users_to_neo4j()